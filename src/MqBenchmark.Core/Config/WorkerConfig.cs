namespace MqBenchmark.Core.Config;

public class WorkerConfig
{
    public static class Roles
    {
        public const string Producer = "Producer";
        public const string Consumer = "Consumer";
        public const string Unknown = "Unknown";
    }
    
    public required string WorkerRole { get; set; }
    public required MqConfig MqConfig { get; set; }
    public required int MessageCount { get; set; }
    public required int MessageSizeInBytes { get; set; }
    public int? SendFrequencyMps { get; set; }
}