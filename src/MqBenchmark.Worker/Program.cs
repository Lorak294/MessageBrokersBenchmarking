using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.Kafka;
using MqBenchmark.Implementations.PgMq;
using MqBenchmark.Implementations.RabbitMq;
using MqBenchmark.Worker;

var builder = Host.CreateApplicationBuilder(args);
RegisterImplementations(builder.Services);

builder.Services.AddSingleton<Worker>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<Worker>());
builder.Services.AddHostedService<OrchestratorClient>();

var host = builder.Build();
host.Run();

static void RegisterImplementations(IServiceCollection services)
{
    services.AddKeyedSingleton<IMqImplementation, KafkaImplementation>(KafkaImplementation.GetKey());
    services.AddKeyedSingleton<IMqImplementation, PgMqImplementation>(PgMqImplementation.GetKey());
    services.AddKeyedSingleton<IMqImplementation, RabbitMqImplementation>(RabbitMqImplementation.GetKey());
}