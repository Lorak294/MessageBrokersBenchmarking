using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi;

namespace MqBenchmark.Orchestrator.OpenApi;

public sealed class InitializeRequestExamplesTransformer : IOpenApiOperationTransformer
{
    public Task TransformAsync(
        OpenApiOperation operation,
        OpenApiOperationTransformerContext context,
        CancellationToken cancellationToken)
    {
        var actionDescriptor = context.Description.ActionDescriptor as ControllerActionDescriptor;
        if (actionDescriptor?.ActionName != "Initialize")
            return Task.CompletedTask;

        if (operation.RequestBody?.Content?.TryGetValue("application/json", out var mediaType) != true)
            return Task.CompletedTask;

        mediaType.Examples = new Dictionary<string, IOpenApiExample>
        {
            ["Kafka PointToPoint"] = CreateExample(
                "Kafka — Point-to-Point, 1 producer, 1 consumer, 10k messages",
                new
                {
                    producersCount = 1,
                    consumerGroups = new[] { 1 },
                    communicationMode = "PointToPoint",
                    messagesPerConsumerGroup = new[] { 10000 },
                    messageSizeInBytes = 1024,
                    sendFrequencyMps = 200,
                    mqConfig = new
                    {
                        implementation = "Kafka",
                        additionalSettings = new Dictionary<string, string>
                        {
                            ["bootstrapServers"] = "kafka:9092",
                            ["lingerMs"] = "5",
                            ["batchSize"] = "65536",
                            ["useBufferedProducer"] = "true"
                        }
                    }
                }),
            ["RabbitMQ PointToPoint"] = CreateExample(
                "RabbitMQ — Point-to-Point, 1 producer, 1 consumer, 1k messages",
                new
                {
                    producersCount = 1,
                    consumerGroups = new[] { 1 },
                    communicationMode = "PointToPoint",
                    messagesPerConsumerGroup = new[] { 1000 },
                    messageSizeInBytes = 1024,
                    sendFrequencyMps = 200,
                    mqConfig = new
                    {
                        implementation = "RabbitMQ",
                        additionalSettings = new Dictionary<string, string>
                        {
                            ["hostname"] = "rabbitmq",
                            ["port"] = "5672",
                            ["username"] = "guest",
                            ["password"] = "guest",
                            ["prefetchCount"] = "100",
                            ["durableMode"] = "false",
                            ["publisherConfirms"] = "false"
                        }
                    }
                }),
            ["PgMq PointToPoint"] = CreateExample(
                "PgMq — Point-to-Point, 1 producer, 1 consumer, 1k messages",
                new
                {
                    producersCount = 1,
                    consumerGroups = new[] { 1 },
                    communicationMode = "PointToPoint",
                    messagesPerConsumerGroup = new[] { 1000 },
                    messageSizeInBytes = 1024,
                    sendFrequencyMps = 200,
                    mqConfig = new
                    {
                        implementation = "PgMq",
                        additionalSettings = new Dictionary<string, string>
                        {
                            ["connectionString"] = "Host=pgmq;Port=5432;Username=postgres;Password=postgres;Database=postgres",
                            ["visibilityTimeout"] = "30",
                            ["queueMode"] = "Default",
                            ["messageReadMode"] = "Delete",
                            ["consumerMode"] = "ClientPoll",
                            ["pollIntervalMs"] = "5",
                            ["usePop"] = "true"
                        }
                    }
                })
        };

        return Task.CompletedTask;
    }

    private static OpenApiExample CreateExample(string summary, object value)
    {
        return new OpenApiExample
        {
            Summary = summary,
            Value = JsonSerializer.SerializeToNode(value, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            })
        };
    }
}
