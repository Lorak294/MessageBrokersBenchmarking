namespace MqBenchmark.Core.MqImplementation;

public record Message
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public required byte[] Payload { get; set;  }
}