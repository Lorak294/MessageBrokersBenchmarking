using MqBenchmark.Core.Constants;
using MqBenchmark.Orchestrator;
using MqBenchmark.Orchestrator.OpenApi;
using MqBenchmark.Orchestrator.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi(options =>
{
    options.AddOperationTransformer<InitializeRequestExamplesTransformer>();
});
builder.Services.AddHealthChecks();
builder.Services.AddSingleton<WorkerRegistry>();
builder.Services.AddSingleton<TimestampAggregator>();
builder.Services.AddSingleton<TestScheduler>();
builder.Services.AddControllers().AddJsonOptions(options =>
{
    options.JsonSerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter());
});
builder.Services.AddSignalR(options =>
{
    // Allow long-running tests without disconnecting workers
    options.ClientTimeoutInterval = TimeSpan.FromMinutes(OrchestratorConstants.ClientTimeoutIntervalMinutes);
    options.KeepAliveInterval = TimeSpan.FromSeconds(OrchestratorConstants.ClientKeepAliveIntervalSeconds);
    options.MaximumReceiveMessageSize = null; // No limit on message size
});

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapControllers();
app.MapHub<OrchestratorHub>("/orchestrator");
// Configure the HTTP request pipeline.
// if (app.Environment.IsDevelopment())
// {
    app.MapOpenApi();
    app.UseSwaggerUI(options =>
    {
        options.SwaggerEndpoint("/openapi/v1.json", "OpenAPI v1");
    });
    //app.UseHttpsRedirection();
// }

app.Run();