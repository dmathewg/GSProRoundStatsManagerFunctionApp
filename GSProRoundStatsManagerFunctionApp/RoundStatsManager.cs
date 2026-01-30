using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using GSProRoundStatsManagerFunctionApp.Models.Stats;
using GSProRoundStatsManagerFunctionApp.Services;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Text.Json;

namespace GSProRoundStatsManagerFunctionApp
{
    public class RoundStatsManager
    {
        private readonly ILogger<RoundStatsManager> _logger;
        private readonly ICosmosDbService<RoundStat> _roundStatService;

        private const string JobStateContainerName = "job-state";
        private const string LastSuccessBlobName = "RoundStatsManager/last-success.json";

        public RoundStatsManager(ILogger<RoundStatsManager> logger,
            ICosmosDbService<RoundStat> roundStatService)
        {
            _logger = logger;
            _roundStatService = roundStatService;
        }

        [Function("RoundStatsManager")]
        public async Task Run(
#if DEBUG
            [TimerTrigger("%CRONSchedule%", RunOnStartup = true)]
        #else
            [TimerTrigger("%CRONSchedule%")]
        #endif
            TimerInfo myTimer)
        {
            // Gets the Calendar instance associated with a CultureInfo.
            CultureInfo cultureInfo = new CultureInfo("en-US");
            Calendar calendar = cultureInfo.Calendar;

            // Gets the DTFI properties required by GetWeekOfYear.
            CalendarWeekRule weekRule = cultureInfo.DateTimeFormat.CalendarWeekRule;
            DayOfWeek firstDOW = cultureInfo.DateTimeFormat.FirstDayOfWeek;
            DateTime dateTime = DateTime.UtcNow;

            int year = dateTime.Year;
            int month = dateTime.Month;
            int week = calendar.GetWeekOfYear(dateTime, weekRule, firstDOW);
            int day = dateTime.Day;

            // Weekly date range (culture-aware)
            int diff = (7 + (dateTime.DayOfWeek - firstDOW)) % 7;
            var startOfWeek = dateTime.Date.AddDays(-diff);
            var endOfWeek = startOfWeek.AddDays(7);

            // Cursor: read last successful finish time from blob (fallback if missing)
            DateTime fallbackSinceUtc = dateTime.AddDays(-8);
            var lastSucceededUtc = await ReadLastSucceededUtcAsync(fallbackSinceUtc);

            // overlap so you don’t miss writes near the boundary
            int overlapSeconds = 300;
            var sinceUtc = lastSucceededUtc.AddSeconds(-overlapSeconds);
            int sinceTs = (int)new DateTimeOffset(sinceUtc).ToUnixTimeSeconds();
            if (sinceTs < 0) sinceTs = 0;

            _logger.LogInformation($"Cursor: lastSucceededUtc={lastSucceededUtc:o}, sinceUtc={sinceUtc:o}, sinceTs={sinceTs}");

            try
            {
                int roundCountRetention = 10;

                // Discover only hashes changed since last success (using _ts)
                string roundFileHashQuery =
                    "select distinct c.RoundFileHash " +
                    "from c " +
                    "where c._ts >= @sinceTs";

                var roundFileHashQueryDefinition = new QueryDefinition(roundFileHashQuery)
                    .WithParameter("@sinceTs", sinceTs);

                var roundFileHashList = (await _roundStatService.QueryItemsAsync(roundFileHashQueryDefinition, null))
                    .Where(r => !string.IsNullOrWhiteSpace(r.RoundFileHash))
                    .Select(r => r.RoundFileHash)
                    .Distinct()
                    .ToList();

                int roundFileHashTotal = roundFileHashList.Count;

                for (int i = 0; i < roundFileHashTotal; i++)
                {
                    var roundFileHash = roundFileHashList[i];
                    int roundFileHashRowNum = i + 1;

                    var rounds = await _roundStatService.ReadFeedAsync(roundFileHash);
                    var roundsList = rounds as List<RoundStat> ?? rounds.ToList();

                    if (roundsList.Count <= roundCountRetention)
                    {
                        _logger.LogInformation($"RoundFileHash: {roundFileHash} - ({roundFileHashRowNum}/{roundFileHashTotal}) - skipped (count={roundsList.Count})");
                        continue;
                    }

                    var allTime = roundsList
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    var monthly = roundsList
                        .Where(s => s.DateModified.Year == year &&
                                    s.DateModified.Month == month)
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    var weekly = roundsList
                        .Where(s => s.DateModified >= startOfWeek && s.DateModified < endOfWeek)
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    var daily = roundsList
                        .Where(s => s.DateModified.Year == year &&
                                    s.DateModified.Month == month &&
                                    s.DateModified.Day == day)
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    var combinedTopRounds = allTime
                        .Concat(monthly)
                        .Concat(weekly)
                        .Concat(daily)
                        .Select(r => r.id)
                        .ToHashSet();

                    var nonTopRounds = roundsList
                        .Where(r => !combinedTopRounds.Contains(r.id))
                        .ToList();

                    // Output results or further processing
                    _logger.LogInformation($"RoundFileHash: {roundFileHash} - ({roundFileHashRowNum}/{roundFileHashTotal})");
                    _logger.LogInformation($"\tTop All Time Rounds: {allTime.Count}");
                    _logger.LogInformation($"\tTop Monthly Rounds: {monthly.Count} ");
                    _logger.LogInformation($"\tTop Weekly Rounds: {weekly.Count} ");
                    _logger.LogInformation($"\tTop Daily Rounds: {daily.Count} ");
                    _logger.LogInformation($"\tNon-Top Rounds: {nonTopRounds.Count}");

                    if (nonTopRounds.Count > 0)
                    {
                        await _roundStatService.DeleteItemsInParallelAsync(nonTopRounds);
                    }
                }

                // Only set cursor AFTER success
                await WriteLastSucceededUtcAsync(DateTime.UtcNow);

                _logger.LogInformation($"Completed at: {DateTime.UtcNow:o}");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex.Message);
            }
        }

        private static async Task<BlobClient> GetLastSuccessBlobClientAsync()
        {
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(JobStateContainerName);

            try
            {
                await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
            }
            catch (RequestFailedException ex) when (ex.Status == 409)
            {
                // Another instance created the container at the same time.
            }

            return containerClient.GetBlobClient(LastSuccessBlobName);
        }

        private async Task<DateTime> ReadLastSucceededUtcAsync(DateTime fallbackUtc)
        {
            try
            {
                var blobClient = await GetLastSuccessBlobClientAsync();

                if (!await blobClient.ExistsAsync())
                {
                    return fallbackUtc;
                }

                var download = await blobClient.DownloadContentAsync();
                var json = download.Value.Content.ToString();

                var state = JsonSerializer.Deserialize<JobState>(json);
                if (state == null || state.LastSucceededUtc == default)
                {
                    return fallbackUtc;
                }

                return DateTime.SpecifyKind(state.LastSucceededUtc, DateTimeKind.Utc);
            }
            catch
            {
                return fallbackUtc;
            }
        }

        private async Task WriteLastSucceededUtcAsync(DateTime finishedUtc)
        {
            var blobClient = await GetLastSuccessBlobClientAsync();

            var state = new JobState
            {
                LastSucceededUtc = finishedUtc,
                LastSucceededTs = (long)new DateTimeOffset(finishedUtc).ToUnixTimeSeconds()
            };

            var json = JsonSerializer.Serialize(state);

            // overwrite marker on each successful run
            await blobClient.UploadAsync(BinaryData.FromString(json), overwrite: true);
        }

        private class JobState
        {
            public DateTime LastSucceededUtc { get; set; }
            public long LastSucceededTs { get; set; }
        }
    }
}