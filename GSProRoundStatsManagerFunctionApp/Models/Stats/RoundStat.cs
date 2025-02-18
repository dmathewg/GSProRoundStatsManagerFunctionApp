using System;

namespace GSProRoundStatsManagerFunctionApp.Models.Stats
{
    public class RoundStat
    {
        public string id { get; set; } = string.Empty;

        public string LicenseKey { get; set; }

        public string SessionKey { get; set; }

        public string RoundKey { get; set; }

        public string PlayerKey { get; set; }

        public string PlayerName { get; set; } = string.Empty;

        public string? RoundType { get; set; }

        public string? RoundName { get; set; }

        public string? RoundFileHash { get; set; }

        public string ControlType { get; set; } = "Simulator";

        public float? ProximityScore { get; set; }

        public float? GIRScore { get; set; }

        public float? StrokesGainedScore { get; set; }

        public float? SkillsScore { get; set; }

        public int? Year { get; set; }

        public int? Week { get; set; }

        public DateTime DateCreated { get; set; } = DateTime.UtcNow;

        public DateTime DateModified { get; set; } = DateTime.UtcNow;

        public int RecordCount { get; set; }
    }
}
