using MqBenchmark.Core.Config;

namespace MqBenchmark.Orchestrator.Contracts;

public class InitializeRequest
{
    public int ConsumersCount { get; set; }
    public int ProducersCount { get; set; }
    
    public int WorkerCount => ConsumersCount + ProducersCount;
    
    public int MessageCount { get; set; }
    
    public int MessageSizeInBytes { get; set; }
    
    public MqConfig MqConfig { get; set; }
}