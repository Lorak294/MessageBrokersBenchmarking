namespace MqBenchmark.Core.Config;

public class WorkerConfig
{
    public enum Role
    {
        Producer,
        Consumer
    }
    
    public required Role WorkerRole { get; set; }
    public required MqConfig MqConfig { get; set; }
    public required int MessageCount { get; set; }
    public required int MessageSizeInBytes { get; set; }
    public int? MessagesPerSecondLimit { get; set; }
}