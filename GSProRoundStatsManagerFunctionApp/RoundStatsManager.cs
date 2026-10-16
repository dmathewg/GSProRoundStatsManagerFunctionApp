using Azure.Identity;
using Azure.Storage.Blobs;
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

        // Each hash is an independent partition, so these fan out cleanly. 8 keeps the
        // in-flight reads well under the Cosmos write limiter and leaves RU headroom
        // for the deletes each iteration kicks off.
        private const int MaxParallelHashes = 8;

        // Container is provisioned outside this app, so no CreateIfNotExists call.
        // Built once per process and reused across invocations. PublicationOnly so a
        // transient failure at first access isn't cached for the life of the process.
        // Deployed: identity-based (AzureWebJobsStorage__accountName; Shared Key is disabled
        // on the account, so a connection string can't work). Local: the Azurite connection
        // string in local.settings.json.
        private static readonly Lazy<BlobClient> _lastSuccessBlobClient = new(() =>
        {
            var accountName = Environment.GetEnvironmentVariable("AzureWebJobsStorage__accountName");
            var service = !string.IsNullOrWhiteSpace(accountName)
                ? new BlobServiceClient(new Uri($"https://{accountName}.blob.core.windows.net"), new ManagedIdentityCredential(ManagedIdentityId.SystemAssigned))
                : new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));

            return service.GetBlobContainerClient(JobStateContainerName).GetBlobClient(LastSuccessBlobName);
        }, LazyThreadSafetyMode.PublicationOnly);

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
            // Gets the CultureInfo used to determine the first day of the week.
            CultureInfo cultureInfo = new CultureInfo("en-US");
            DayOfWeek firstDOW = cultureInfo.DateTimeFormat.FirstDayOfWeek;
            DateTime dateTime = DateTime.UtcNow;

            int year = dateTime.Year;
            int month = dateTime.Month;
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

            _logger.LogInformation("Cursor: lastSucceededUtc={LastSucceededUtc:o}, sinceUtc={SinceUtc:o}, sinceTs={SinceTs}",
                lastSucceededUtc, sinceUtc, sinceTs);

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
                _logger.LogInformation("Hash discovery: {HashCount} hashes changed since {SinceUtc:o}", roundFileHashTotal, sinceUtc);

                // Was a sequential for-loop: one ReadFeedAsync round trip awaited at a time,
                // so total runtime was roundFileHashTotal x per-read latency. Fanning out
                // turns that into (total / MaxParallelHashes) x latency.
                int processed = 0;
                int skipped = 0;
                int deletedTotal = 0;

                await Parallel.ForEachAsync(
                    roundFileHashList,
                    new ParallelOptions { MaxDegreeOfParallelism = MaxParallelHashes },
                    async (roundFileHash, ct) =>
                    {
                        var rounds = await _roundStatService.ReadFeedAsync(roundFileHash);
                        var roundsList = rounds as List<RoundStat> ?? rounds.ToList();

                        if (roundsList.Count <= roundCountRetention)
                        {
                            Interlocked.Increment(ref skipped);
                            return;
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

                        // Output results or further processing.
                        // Single line per hash now - interleaved parallel output made the
                        // old six-line block unreadable, and the counts are all here anyway.
                        _logger.LogInformation(
                            "RoundFileHash: {Hash} - rounds={Rounds} allTime={AllTime} monthly={Monthly} weekly={Weekly} daily={Daily} nonTop={NonTop}",
                            roundFileHash, roundsList.Count, allTime.Count, monthly.Count, weekly.Count, daily.Count, nonTopRounds.Count);

                        if (nonTopRounds.Count > 0)
                        {
                            await _roundStatService.DeleteItemsInParallelAsync(nonTopRounds);
                            Interlocked.Add(ref deletedTotal, nonTopRounds.Count);
                        }

                        Interlocked.Increment(ref processed);
                    });

                _logger.LogInformation(
                    "Hashes: total={Total} processed={Processed} skipped={Skipped} roundsDeleted={Deleted}",
                    roundFileHashTotal, processed, skipped, deletedTotal);

                // Only set cursor AFTER success. Use the run's START time so docs written
                // while the run was in flight aren't skipped on the next tick.
                await WriteLastSucceededUtcAsync(dateTime);

                _logger.LogInformation("Completed at: {CompletedUtc:o}", DateTime.UtcNow);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RoundStatsManager run failed; cursor not advanced.");
            }
        }

        private async Task<DateTime> ReadLastSucceededUtcAsync(DateTime fallbackUtc)
        {
            try
            {
                var blobClient = _lastSuccessBlobClient.Value;

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
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read cursor blob; falling back to {FallbackUtc:o}", fallbackUtc);
                return fallbackUtc;
            }
        }

        private async Task WriteLastSucceededUtcAsync(DateTime finishedUtc)
        {
            var blobClient = _lastSuccessBlobClient.Value;

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