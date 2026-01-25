using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Core.Config;
using MqBenchmark.Orchestrator.Contracts;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;

[ApiController]
[Route("api/[controller]")]
public class BenchmarkingController : ControllerBase
{
    private readonly ILogger<BenchmarkingController> _logger;
    private readonly WorkerRegistry _workerRegistry;
    private readonly IHubContext<OrchestratorHub> _hubContext;
    
    public BenchmarkingController(
        ILogger<BenchmarkingController> logger, 
        WorkerRegistry workerRegistry, 
        IHubContext<OrchestratorHub> hubContext)
    {
        _logger = logger;
        _workerRegistry = workerRegistry;
        _hubContext = hubContext;
    }
    
    [HttpPost("initialize")]
    public async Task<IActionResult> Initialize([FromBody] InitializeRequest request)
    {
        int workerCount = _workerRegistry.ConnectedWorkerCount;
        if (workerCount == 0)
        {
            return BadRequest("No workers connected.");
        }
        
        _logger.LogInformation("Initializing test with config: {@TestConfig}", request);
        
        // TODO: split workers into consumers and producers based on request
        
        // reset and broadcast reinitialization to workers
        _workerRegistry.ResetReadiness();
        await _hubContext.Clients.Group("worker").SendAsync(OrchestratorMethods.InitializeTest, new TestConfig
        {
            MessageCount =  request.MessageCount,
            MessageSizeInBytes = request.MessageSizeInBytes,
            MqConfig = request.MqConfig,
        });
        
        // wait for acknowledgments from all workers
        try
        {
            await _workerRegistry.WaitForAllWorkersReadyAsync(TimeSpan.FromSeconds(30));
        }
        catch (TimeoutException)
        {
            return StatusCode(504, "Timeout waiting for workers to initialize.");
        }
        return Ok(new { Message = $"Initialized {workerCount} workers successfully." });
    }
}