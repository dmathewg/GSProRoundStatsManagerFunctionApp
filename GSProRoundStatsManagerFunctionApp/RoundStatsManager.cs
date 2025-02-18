using GSProRoundStatsManagerFunctionApp.Models.Stats;
using GSProRoundStatsManagerFunctionApp.Services;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Globalization;

namespace GSProRoundStatsManagerFunctionApp
{
    public class RoundStatsManager
    {
        private readonly ILogger<RoundStatsManager> _logger;
        private readonly ICosmosDbService<RoundStat> _roundStatService;

        public RoundStatsManager(ILogger<RoundStatsManager> logger,
            ICosmosDbService<RoundStat> roundStatService)
        {
            _logger = logger;
            _roundStatService = roundStatService;
        }

        [Function("RoundStatsManager")]
        public async Task Run([TimerTrigger("%CRONSchedule%")] TimerInfo myTimer)
        {
            // Gets the Calendar instance associated with a CultureInfo.
            CultureInfo cultureInfo = new CultureInfo("en-US");
            Calendar calendar = cultureInfo.Calendar;

            // Gets the DTFI properties required by GetWeekOfYear.
            CalendarWeekRule weekRule = cultureInfo.DateTimeFormat.CalendarWeekRule;
            DayOfWeek firstDOW = cultureInfo.DateTimeFormat.FirstDayOfWeek;
            DateTime dateTime = DateTime.UtcNow;

        int year = dateTime.Year;
            int week = calendar.GetWeekOfYear(dateTime, weekRule, firstDOW);

            try
            {
                int roundCountRetention = 10;
                string roundFileHashQuery = "select c.RoundFileHash, count(1) as RecordCount from c group by c.RoundFileHash"; // where c.RoundFileHash = '134936490905'";
                var roundFileHashQueryDefinition = new QueryDefinition(roundFileHashQuery);

                var roundFileHashList = (await _roundStatService.QueryItemsAsync(roundFileHashQueryDefinition, null))
                    .Where(r => r.RecordCount > roundCountRetention) // Don't process any RoundFileHash if total records are below roundCountRetention
                    .OrderBy(r => r.RecordCount)
                    .Select(r => r.RoundFileHash)
                    .ToList();

                int roundFileHashRowNum = 0;
                int roundFileHashTotal = roundFileHashList.Count;

                foreach(var roundFileHash in roundFileHashList)
                {
                    roundFileHashRowNum = roundFileHashList.IndexOf(roundFileHash) + 1;

                    var rounds = await _roundStatService.ReadFeedAsync(roundFileHash);

                    var allTime = rounds
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    var monthly = rounds
                        .Where(s => s.DateModified.Year == dateTime.Year &&
                                    s.DateModified.Month == dateTime.Month)
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    var weekly = rounds
                        .Where(s => s.DateModified.Year == dateTime.Year &&
                                    calendar.GetWeekOfYear(s.DateModified, weekRule, firstDOW) == calendar.GetWeekOfYear(dateTime, weekRule, firstDOW))
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    var daily = rounds
                        .Where(s => s.DateModified.Year == dateTime.Year &&
                                    s.DateModified.Month == dateTime.Month &&
                                    s.DateModified.Day == dateTime.Day)
                        .GroupBy(r => new { r.LicenseKey, r.ControlType })
                        .SelectMany(g => g
                            .OrderByDescending(r => r.SkillsScore)
                            .ThenByDescending(r => r.DateModified)
                            .Take(roundCountRetention))
                        .ToList();

                    // Combine top rounds from all periods
                    var combinedTopRounds = allTime
                        .Concat(monthly)
                        .Concat(weekly)
                        .Concat(daily)
                        .Select(r => r.id)
                        .Distinct()
                        .ToList();

                    // Find all rows that don't fit into the top rounds criteria
                    var nonTopRounds = rounds
                        .Where(r => !combinedTopRounds.Contains(r.id))
                        .OrderByDescending(r => r.SkillsScore)
                        .ThenByDescending(r => r.DateModified)
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
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex.Message);
            }

            _logger.LogInformation($"Completed at: {DateTime.UtcNow}");
        }
    }
}
