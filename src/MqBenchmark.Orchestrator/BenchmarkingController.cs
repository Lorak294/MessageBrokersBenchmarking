using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Core.Config;
using MqBenchmark.Orchestrator.Contracts;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;

[ApiController]
[Route("api/[controller]")]
public class BenchmarkingController(
    ILogger<BenchmarkingController> logger,
    TestScheduler testScheduler)
    : ControllerBase
{
    [HttpPost("initialize")]
    public async Task<IActionResult> Initialize([FromBody] InitializeRequest request)
    {
        logger.LogInformation("Initializing test with config: {@TestConfig}", request);
        
        try
        {
            await testScheduler.InitializeTestAsync(request);
        }
        catch (TimeoutException)
        {
            logger.LogError("Timeout waiting for workers to acknowledge initialization.");
            return StatusCode(504, "Timeout waiting for workers to initialize.");
        }
        catch (Exception ex)
        {
            return StatusCode(500, ex.Message);
        }
        return Ok(new { Message = $" Test initialized successfully." });
    }
    
    [HttpPost("start")]
    public async Task<IActionResult> Start()
    {
        logger.LogInformation("Starting benchmark test...");
        try
        {
            await testScheduler.StartTestAsync();
        }
        catch (Exception ex)
        {
            return StatusCode(500, ex.Message);
        }
        return Ok(new { Message = "Benchmark test started successfully." });
    }
}