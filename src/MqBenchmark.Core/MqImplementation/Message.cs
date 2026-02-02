namespace MqBenchmark.Core.MqImplementation;

public record Message
{
    private const int IdSize = 16; // Guid is 16 bytes
    public Guid Id => new(Payload.AsSpan(0, IdSize));
    public required byte[] Payload { get; set;  }

    private Message() {}

    public static Message CreateMessage(int byteSize)
    {
        if(byteSize < IdSize)
        {
            throw new ArgumentException($"Message size must be at least {IdSize} bytes to accommodate the ID.");
        }
        
        var payload = new byte[byteSize];
        var id = Guid.NewGuid();
        if (!id.TryWriteBytes(payload.AsSpan(0, IdSize)))
        {
            throw new InvalidOperationException($"Failed to encode ID: {id} into payload.");
        }
        
        return new Message
        {
            Payload = payload
        };
    }

    public static Message FromBytes(byte[] bytes)
    {
        return new Message()
        {
            Payload = bytes
        };
    }
}