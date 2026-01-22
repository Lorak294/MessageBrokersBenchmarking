using Microsoft.AspNetCore.Mvc;
using MqBenchmark.Orchestrator.Contracts;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;

[ApiController]
[Route("api/[controller]")]
public class BenchmarkingController(
    ILogger<BenchmarkingController> logger,
    TestScheduler testScheduler,
    TimestampAggregator timestampAggregator)
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
    /// Gets the aggregated benchmark results from the last completed test.
    /// </summary>
    [HttpGet("results")]
    public IActionResult GetResults()
    {
        try
        {
            var results = timestampAggregator.ComputeResults();
            return Ok(results);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error computing benchmark results");
            return StatusCode(500, ex.Message);
        }
    }
    
    /// <summary>
    /// Gets the raw timestamp data from all workers.
    /// </summary>
    [HttpGet("timestamps")]
    public IActionResult GetTimestamps()
    {
        try
        {
            var timestamps = timestampAggregator.GetAllTimestampData();
            return Ok(timestamps);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error retrieving timestamp data");
            return StatusCode(500, ex.Message);
        }
    }
}