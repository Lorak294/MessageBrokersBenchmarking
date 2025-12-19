namespace MqBenchmark.Core.MqImplementation;

public record Message
{
    public required byte[] Payload { get; set;  }
}