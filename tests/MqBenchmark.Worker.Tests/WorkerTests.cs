using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Worker.Tests;

public class WorkerTests
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IMqImplementation _mqImplementation;
    private readonly IMqProducer _producer;
    private readonly IMqConsumer _consumer;
    private readonly IMqJanitor _janitor;
    private readonly Worker _worker;

    public WorkerTests()
    {
        _mqImplementation = Substitute.For<IMqImplementation>();
        _producer = Substitute.For<IMqProducer>();
        _consumer = Substitute.For<IMqConsumer>();
        _janitor = Substitute.For<IMqJanitor>();

        _mqImplementation.CreateProducer().Returns(_producer);
        _mqImplementation.CreateConsumer().Returns(_consumer);
        _mqImplementation.CreateJanitor().Returns(_janitor);

        var sp = Substitute.For<IServiceProvider, IKeyedServiceProvider, IServiceProviderIsKeyedService>();
        ((IKeyedServiceProvider)sp).GetKeyedService(typeof(IMqImplementation), "Kafka").Returns(_mqImplementation);
        ((IKeyedServiceProvider)sp).GetRequiredKeyedService(typeof(IMqImplementation), "Kafka").Returns(_mqImplementation);
        ((IServiceProviderIsKeyedService)sp).IsKeyedService(typeof(IMqImplementation), "Kafka").Returns(true);
        _serviceProvider = sp;

        var logger = Substitute.For<ILogger<Worker>>();
        _worker = new Worker(logger, sp);
    }

    [Fact]
    public async Task InitializeTestAsync_AsProducer_CreatesProducer()
    {
        // Arrange
        var config = CreateConfig(WorkerConfig.Roles.Producer);

        // Act
        await _worker.InitializeTestAsync(config);

        // Assert
        _mqImplementation.Received(1).CreateProducer();
        await _producer.Received(1).InitializeAsync(config.MqConfig);
    }

    [Fact]
    public async Task InitializeTestAsync_AsConsumer_CreatesConsumer()
    {
        // Arrange
        var config = CreateConfig(WorkerConfig.Roles.Consumer);

        // Act
        await _worker.InitializeTestAsync(config);

        // Assert
        _mqImplementation.Received(1).CreateConsumer();
        await _consumer.Received(1).InitializeAsync(config.MqConfig);
    }

    [Fact]
    public async Task InitializeTestAsync_InvalidRole_Throws()
    {
        // Arrange
        var config = CreateConfig("InvalidRole");

        // Act
        var act = () => _worker.InitializeTestAsync(config);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*Unsupported worker role*");
    }

    [Fact]
    public async Task StartTestAsync_WithoutInit_Throws()
    {
        // Act
        var act = () => _worker.StartTestAsync();

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not initialized*");
    }

    [Fact]
    public async Task PrepareInfrastructureAsync_CallsJanitorAndDisposes()
    {
        // Arrange
        var config = new JanitorConfig
        {
            MqConfig = new MqConfig { Implementation = "Kafka" },
            CommunicationMode = CommunicationMode.PointToPoint,
            ConsumerGroups = [2]
        };

        // Act
        await _worker.PrepareInfrastructureAsync(config);

        // Assert
        _mqImplementation.Received(1).CreateJanitor();
        await _janitor.Received(1).PrepareInfrastructureAsync(config);
        await _janitor.Received(1).DisposeAsync();
    }

    [Fact]
    public void GetTimestampData_ReturnsCorrectGroupIndex()
    {
        // Act — without initialization, role is Unknown and group is 0
        var data = _worker.GetTimestampData();

        // Assert
        data.Role.Should().Be(WorkerConfig.Roles.Unknown);
        data.ConsumerGroupIndex.Should().Be(0);
        data.WorkerId.Should().Be(_worker.Id);
    }

    private static WorkerConfig CreateConfig(string role) => new()
    {
        WorkerRole = role,
        MqConfig = new MqConfig
        {
            Implementation = "Kafka",
            ConsumerGroupName = "group_1",
            ConsumerGroupCount = 2
        },
        MessageCount = 100,
        MessageSizeInBytes = 64
    };
}
