using System.IO.Compression;
using System.Text.Json;

namespace MqBenchmark.Core.Metrics;

public static class TimestampBatchTransfer
{
    public const int DefaultBatchSize = 5_000;
    
    public static List<CompressedTimestampBatch> CompressBatches(WorkerTimestampData data, int batchSize = DefaultBatchSize)
    {
        var batches = new List<CompressedTimestampBatch>();
        var timestamps = data.Timestamps;
        int totalBatches = (int)Math.Ceiling((double)timestamps.Count / batchSize);
        if (totalBatches == 0) totalBatches = 1;

        for (int i = 0; i < totalBatches; i++)
        {
            var chunk = timestamps.Skip(i * batchSize).Take(batchSize).ToList();

            var batchData = data with { Timestamps = chunk };

            var json = JsonSerializer.SerializeToUtf8Bytes(batchData);
            var compressed = GZipCompress(json);

            batches.Add(new CompressedTimestampBatch
            {
                WorkerId = data.WorkerId,
                BatchIndex = i,
                TotalBatches = totalBatches,
                CompressedData = compressed
            });
        }

        return batches;
    }
    
    public static WorkerTimestampData DecompressBatch(CompressedTimestampBatch batch)
    {
        var json = GZipDecompress(batch.CompressedData);
        return JsonSerializer.Deserialize<WorkerTimestampData>(json)
               ?? throw new InvalidOperationException("Failed to deserialize timestamp batch.");
    }

    private static byte[] GZipCompress(byte[] data)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.SmallestSize, leaveOpen: true))
        {
            gzip.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }

    private static byte[] GZipDecompress(byte[] compressed)
    {
        using var input = new MemoryStream(compressed);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return output.ToArray();
    }
}

public record CompressedTimestampBatch
{
    public required Guid WorkerId { get; init; }
    public required int BatchIndex { get; init; }
    public required int TotalBatches { get; init; }
    public required byte[] CompressedData { get; init; }
}
