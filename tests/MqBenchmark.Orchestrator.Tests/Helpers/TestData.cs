using MqBenchmark.Core.Config;
using MqBenchmark.Orchestrator.Contracts;

namespace MqBenchmark.Orchestrator.Tests.Helpers;

public static class TestData
{
    public static InitializeRequest ValidPointToPointRequest() => new()
    {
        ProducersCount = 2,
        ConsumerGroups = [2, 3],
        CommunicationMode = CommunicationMode.PointToPoint,
        MessageCount = 1000,
        MessageSizeInBytes = 64,
        MqConfig = new MqConfig { Implementation = "Kafka" }
    };

    public static InitializeRequest ValidPubSubRequest() => new()
    {
        ProducersCount = 1,
        ConsumerGroups = [1, 1],
        CommunicationMode = CommunicationMode.PubSub,
        MessageCount = 500,
        MessageSizeInBytes = 128,
        MqConfig = new MqConfig { Implementation = "RabbitMQ" }
    };

    public static InitializeRequest ValidStreamingRequest() => new()
    {
        ProducersCount = 1,
        ConsumerGroups = [1, 1],
        CommunicationMode = CommunicationMode.Streaming,
        MessageCount = 500,
        MessageSizeInBytes = 32,
        MqConfig = new MqConfig { Implementation = "Kafka" }
    };
}
