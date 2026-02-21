using MqBenchmark.Orchestrator;
using MqBenchmark.Orchestrator.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddSingleton<WorkerRegistry>();
builder.Services.AddSingleton<TimestampAggregator>();
builder.Services.AddSingleton<TestScheduler>();
builder.Services.AddControllers();
builder.Services.AddSignalR();

var app = builder.Build();
app.MapControllers();
app.MapHub<OrchestratorHub>("/orchestrator");
// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.UseSwaggerUI(options =>
    {
        options.SwaggerEndpoint("/openapi/v1.json", "OpenAPI v1");
    });
}

app.UseHttpsRedirection();
app.Run();