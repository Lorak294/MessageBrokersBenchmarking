using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Implementations.Kafka;
using MqBenchmark.Implementations.PgMq;
using MqBenchmark.Implementations.RabbitMq;
using Xunit;

namespace MqBenchmark.Implementations.Tests;

public class NamingTests
{
    [Fact]
    public void KafkaNaming_GroupTopic_ReturnsExpectedFormat()
    {
        // Act & Assert
        KafkaNaming.GroupTopic("group_0").Should().Be("benchmark_group_0");
    }

    [Fact]
    public void KafkaNaming_SharedTopic_ReturnsExpectedFormat()
    {
        // Act & Assert
        KafkaNaming.SharedTopic().Should().Be("benchmark");
    }

    [Fact]
    public void KafkaNaming_GroupId_ReturnsExpectedFormat()
    {
        // Act & Assert
        KafkaNaming.GroupId("group_1").Should().Be("benchmark_group_1");
    }

    [Fact]
    public void RabbitMqNaming_GroupQueue_ReturnsExpectedFormat()
    {
        // Act & Assert
        RabbitMqNaming.GroupQueue("group_0").Should().Be("benchmark_group_0");
    }

    [Fact]
    public void RabbitMqNaming_TopicExchange_ReturnsExpectedFormat()
    {
        // Act & Assert
        RabbitMqNaming.TopicExchange().Should().Be("benchmark_topic");
    }

    [Fact]
    public void RabbitMqNaming_FanoutExchange_ReturnsExpectedFormat()
    {
        // Act & Assert
        RabbitMqNaming.FanoutExchange().Should().Be("benchmark_fanout");
    }

    [Fact]
    public void RabbitMqNaming_StreamQueue_ReturnsExpectedFormat()
    {
        // Act & Assert
        RabbitMqNaming.StreamQueue().Should().Be("benchmark_stream");
    }

    [Fact]
    public void PgMqNaming_GroupQueue_ReturnsExpectedFormat()
    {
        // Act & Assert
        PgMqNaming.GroupQueue("group_0").Should().Be("benchmark_group_0");
    }

    [Fact]
    public void PgMqNaming_StreamQueue_ReturnsExpectedFormat()
    {
        // Act & Assert
        PgMqNaming.StreamQueue().Should().Be("benchmark_stream");
    }

    [Fact]
    public void PgMqNaming_BroadcastRoutingKey_ReturnsExpectedValue()
    {
        // Act & Assert
        PgMqNaming.BroadcastRoutingKey().Should().Be("broadcast");
    }

    [Fact]
    public void PgMqNaming_GroupRoutingKey_ReturnsGroupName()
    {
        // Act & Assert
        PgMqNaming.GroupRoutingKey("group_2").Should().Be("group_2");
    }
}

public class ConfigParsingTests
{
    [Fact]
    public void ToKafkaConfig_ParsesAllSettings()
    {
        // Arrange
        var mqConfig = new MqConfig
        {
            Implementation = "Kafka",
            AdditionalSettings = new Dictionary<string, string>
            {
                ["bootstrapServers"] = "broker1:9092,broker2:9092",
                ["lingerMs"] = "10",
                ["batchSize"] = "131072",
                ["useBufferedProducer"] = "false"
            }
        };

        // Act
        var result = mqConfig.ToKafkaConfig();

        // Assert
        result.BootstrapServers.Should().Be("broker1:9092,broker2:9092");
        result.LingerMs.Should().Be(10);
        result.BatchSize.Should().Be(131072);
        result.UseBufferedProducer.Should().BeFalse();
    }

    [Fact]
    public void ToKafkaConfig_UsesDefaults_WhenSettingsOmitted()
    {
        // Arrange
        var mqConfig = new MqConfig
        {
            Implementation = "Kafka",
            AdditionalSettings = new Dictionary<string, string>
            {
                ["bootstrapServers"] = "localhost:9092"
            }
        };

        // Act
        var result = mqConfig.ToKafkaConfig();

        // Assert
        result.LingerMs.Should().Be(5);
        result.BatchSize.Should().Be(65536);
        result.UseBufferedProducer.Should().BeTrue();
    }

    [Fact]
    public void ToRabbitMqConfig_ParsesAllSettings()
    {
        // Arrange
        var mqConfig = new MqConfig
        {
            Implementation = "RabbitMQ",
            AdditionalSettings = new Dictionary<string, string>
            {
                ["hostname"] = "rabbit-host",
                ["port"] = "5673",
                ["username"] = "admin",
                ["password"] = "secret",
                ["durableMode"] = "true",
                ["prefetchCount"] = "200",
                ["publisherConfirms"] = "true"
            }
        };

        // Act
        var result = mqConfig.ToRabbitMqConfig();

        // Assert
        result.Hostname.Should().Be("rabbit-host");
        result.Port.Should().Be(5673);
        result.Username.Should().Be("admin");
        result.Password.Should().Be("secret");
        result.DurableMode.Should().BeTrue();
        result.PrefetchCount.Should().Be(200);
        result.PublisherConfirms.Should().BeTrue();
    }

    [Fact]
    public void ToPgMqConfig_ParsesAllSettings()
    {
        // Arrange
        var mqConfig = new MqConfig
        {
            Implementation = "PgMq",
            AdditionalSettings = new Dictionary<string, string>
            {
                ["connectionString"] = "Host=db;Database=pgmq",
                ["visibilityTimeout"] = "60",
                ["queueMode"] = "Unlogged",
                ["messageReadMode"] = "Archive",
                ["consumerMode"] = "ServerPoll",
                ["pollIntervalMs"] = "10",
                ["maxPollSeconds"] = "10",
                ["usePop"] = "false",
                ["useBufferedProducer"] = "true",
                ["producerBatchSize"] = "200",
                ["producerLingerMs"] = "10",
                ["consumerBatchSize"] = "5"
            }
        };

        // Act
        var result = mqConfig.ToPgMqConfig();

        // Assert
        result.ConnectionString.Should().Be("Host=db;Database=pgmq");
        result.VisibilityTimeout.Should().Be(60);
        result.QueueMode.Should().Be(PgMqConfig.QueueModeEnum.Unlogged);
        result.MessageReadMode.Should().Be(PgMqConfig.ReadModeEnum.Archive);
        result.ConsumerMode.Should().Be(PgMqConfig.ConsumerModeEnum.ServerPoll);
        result.PollIntervalMs.Should().Be(10);
        result.MaxPollSeconds.Should().Be(10);
        result.UsePop.Should().BeFalse();
        result.UseBufferedProducer.Should().BeTrue();
        result.ProducerBatchSize.Should().Be(200);
        result.ProducerLingerMs.Should().Be(10);
        result.ConsumerBatchSize.Should().Be(5);
    }

    [Fact]
    public void ToKafkaConfig_MissingRequiredSetting_Throws()
    {
        // Arrange
        var mqConfig = new MqConfig
        {
            Implementation = "Kafka",
            AdditionalSettings = new Dictionary<string, string>()
        };

        // Act
        var act = () => mqConfig.ToKafkaConfig();

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*bootstrapServers*");
    }
}
