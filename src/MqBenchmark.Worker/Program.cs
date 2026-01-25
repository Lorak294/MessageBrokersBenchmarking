using MqBenchmark.Implementations.Dummy;
using MqBenchmark.Core.MqImplementation;
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
    services.AddKeyedSingleton<IMqImplementation, DummyImplementation>(DummyImplementation.GetKey());
    //services.AddKeyedSingleton<IMqImplementation, RabbitMqImplementation>(RabbitMqImplementation.GetKey());
}