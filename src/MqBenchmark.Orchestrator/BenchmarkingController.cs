using Microsoft.AspNetCore.Mvc;
using MqBenchmark.Orchestrator.Contracts;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;

[ApiController]
[Route("api/[controller]")]
public class BenchmarkingController(
    ILogger<BenchmarkingController> logger,
    TestScheduler testScheduler,
    TimestampAggregator timestampAggregator,
    WorkerRegistry workerRegistry)
    : ControllerBase
{
    /// <summary>
    /// Gets the list of currently connected workers and their state.
    /// </summary>
    [HttpGet("workers")]
    public IActionResult GetWorkers()
    {
        var workers = workerRegistry.GetAllWorkers();
        return Ok(workers);
    }

    /// <summary>
    /// Resets broker environment and initializes test scenario based on configuration.
    /// </summary>
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
    
    /// <summary>
    /// Starts execution of currently initialized test scenario.
    /// </summary>
    [HttpPost("start")]
    public async Task<IActionResult> Start()
    {
        logger.LogInformation("Starting benchmark test...");
        try
        {
            var results = await testScheduler.StartTestAsync();
            return Ok(new 
            { 
                Message = "Benchmark test completed successfully.",
                Results = results
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, ex.Message);
        }
    }
    
    /// <summary>
    /// Downloads the full results file (aggregations + per-message latencies) by filename.
    /// </summary>
    [HttpGet("results/{fileName}")]
    public IActionResult GetResults(string fileName)
    {
        var filePath = timestampAggregator.GetResultsFilePath(fileName);
        if (filePath is null)
        {
            return NotFound($"Results file '{fileName}' not found.");
        }

        return PhysicalFile(Path.GetFullPath(filePath), "text/csv", fileName);
    }
}
