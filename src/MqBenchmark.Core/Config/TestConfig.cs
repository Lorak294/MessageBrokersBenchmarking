namespace MqBenchmark.Core.Config;

public class TestConfig
{
    // TODO: reeplace with enum/config section
    public bool IsConsumer { get; set; }
    
    public required int MessageCount { get; set; }
    public required int MessageSizeInBytes { get; set; }
    public required MqConfig MqConfig { get; set; }
}