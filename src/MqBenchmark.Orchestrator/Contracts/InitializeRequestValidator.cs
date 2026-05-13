using MqBenchmark.Core.Config;

namespace MqBenchmark.Orchestrator.Contracts;

public static class InitializeRequestValidator
{
    private static readonly HashSet<string> ValidImplementations = ["Kafka", "RabbitMQ", "PgMq"];

    public static List<string> Validate(InitializeRequest request)
    {
        var errors = new List<string>();

        if (!Enum.IsDefined(request.CommunicationMode))
            errors.Add("communicationMode must be a valid value (PointToPoint, PubSub, Streaming).");

        if (request.ProducersCount <= 0)
            errors.Add("producersCount must be greater than 0.");

        if (request.ConsumerGroups is not { Length: > 0 })
        {
            errors.Add("consumerGroups must be a non-empty array.");
        }
        else
        {
            for (int i = 0; i < request.ConsumerGroups.Length; i++)
            {
                if (request.ConsumerGroups[i] <= 0)
                    errors.Add($"consumerGroups[{i}] must be greater than 0.");
            }
        }

        if (request.MessageSizeInBytes < 16)
            errors.Add("messageSizeInBytes must be at least 16.");

        if (request.SendFrequencyMps is <= 0)
            errors.Add("sendFrequencyMps must be greater than 0 when specified.");

        // Mode-specific message count validation
        switch (request.CommunicationMode)
        {
            case CommunicationMode.PointToPoint:
                if (request.MessageCount is not null && request.MessagesPerConsumerGroup is not null)
                    errors.Add("For PointToPoint mode, set either messageCount or messagesPerConsumerGroup, not both.");
                else if (request.MessageCount is null or <= 0 && request.MessagesPerConsumerGroup is null)
                    errors.Add("For PointToPoint mode, either messageCount or messagesPerConsumerGroup must be set.");

                if (request.MessagesPerConsumerGroup is not null)
                {
                    if (request.ConsumerGroups is { Length: > 0 }
                        && request.MessagesPerConsumerGroup.Length != request.ConsumerGroups.Length)
                    {
                        errors.Add(
                            $"messagesPerConsumerGroup length ({request.MessagesPerConsumerGroup.Length}) " +
                            $"must match consumerGroups length ({request.ConsumerGroups.Length}).");
                    }

                    for (int i = 0; i < request.MessagesPerConsumerGroup.Length; i++)
                    {
                        if (request.MessagesPerConsumerGroup[i] <= 0)
                            errors.Add($"messagesPerConsumerGroup[{i}] must be greater than 0.");
                    }
                }
                break;

            case CommunicationMode.PubSub:
            case CommunicationMode.Streaming:
                if (request.MessageCount is null or <= 0)
                    errors.Add($"For {request.CommunicationMode} mode, messageCount must be set and greater than 0.");
                break;
        }

        // MqConfig validation
        if (request.MqConfig is null)
        {
            errors.Add("mqConfig is required.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(request.MqConfig.Implementation))
                errors.Add("mqConfig.implementation is required.");
            else if (!ValidImplementations.Contains(request.MqConfig.Implementation))
                errors.Add($"mqConfig.implementation must be one of: {string.Join(", ", ValidImplementations)}.");
        }

        // Streaming mode constraint: RabbitMQ and PGMQ don't support competing consumers
        if (request.CommunicationMode == CommunicationMode.Streaming
            && request.MqConfig?.Implementation is "RabbitMQ" or "PgMq"
            && request.ConsumerGroups is { Length: > 0 })
        {
            var invalidGroups = request.ConsumerGroups.Where(g => g > 1).ToArray();
            if (invalidGroups.Length > 0)
            {
                errors.Add(
                    $"Streaming mode for {request.MqConfig.Implementation} does not support multiple consumers per group. " +
                    $"Use 1 consumer per group instead of [{string.Join(", ", request.ConsumerGroups)}].");
            }
        }

        return errors;
    }
}
