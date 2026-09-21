using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Estornos.TestRunner;

public static class BenchmarkEngine
{
    private static readonly HttpClient HttpClientInstance = new(new SocketsHttpHandler
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(15),
        MaxConnectionsPerServer = 200
    })
    {
        Timeout = TimeSpan.FromSeconds(60) // Timeout HTTP de 60s explicitamente configurado no cliente da PoC
    };

    public static async Task<TestMetrics> SendConstantRateLoadAsync(string url, int ratePerSecond, TimeSpan duration, CancellationToken token)
    {
        var resultsBag = new ConcurrentBag<RequestResult>();
        int sent = 0;
        int success = 0;
        int failure = 0;

        var stopwatch = Stopwatch.StartNew();
        var tasks = new List<Task>();
        double intervalMs = 1000.0 / ratePerSecond;

        long nextSendTicks = stopwatch.ElapsedTicks;
        double ticksPerMs = Stopwatch.Frequency / 1000.0;

        int totalTargetRequests = (int)Math.Round(ratePerSecond * duration.TotalSeconds);
        int requestId = 0;

        while (requestId < totalTargetRequests && !token.IsCancellationRequested)
        {
            Interlocked.Increment(ref sent);
            int currentId = requestId++;

            tasks.Add(Task.Run(async () =>
            {
                var reqStopwatch = Stopwatch.StartNew();
                double startTimeMs = stopwatch.Elapsed.TotalMilliseconds;
                bool isSuccess = false;

                using var reqCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token, reqCts.Token);

                try
                {
                    var payload = new RequestEstorno($"TX-{Guid.NewGuid()}", 50.00m, $"Teste Carga {currentId}");
                    var json = JsonSerializer.Serialize(payload);
                    using var content = new StringContent(json, Encoding.UTF8, "application/json");

                    var response = await HttpClientInstance.PostAsync(url, content, linkedCts.Token);
                    reqStopwatch.Stop();
                    isSuccess = response.IsSuccessStatusCode;

                    if (isSuccess) Interlocked.Increment(ref success);
                    else Interlocked.Increment(ref failure);
                }
                catch
                {
                    reqStopwatch.Stop();
                    Interlocked.Increment(ref failure);
                }
                finally
                {
                    double latency = reqStopwatch.Elapsed.TotalMilliseconds;
                    resultsBag.Add(new RequestResult
                    {
                        StartTimeMs = startTimeMs,
                        LatencyMs = latency,
                        IsSuccess = isSuccess
                    });
                }
            }, token));

            nextSendTicks += (long)(intervalMs * ticksPerMs);
            long sleepTicks = nextSendTicks - stopwatch.ElapsedTicks;
            if (sleepTicks > 0)
            {
                int sleepMs = (int)(sleepTicks / ticksPerMs);
                if (sleepMs > 0)
                {
                    try
                    {
                        await Task.Delay(sleepMs, token);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                }
            }
        }

        try
        {
            await Task.WhenAll(tasks);
        }
        catch
        {
            // Ignora exceções agregadas de cancelamento
        }

        stopwatch.Stop();
        return CalculateMetrics(sent, success, failure, resultsBag.ToList(), stopwatch.Elapsed.TotalSeconds);
    }

    private static TestMetrics CalculateMetrics(int sent, int success, int failure, List<RequestResult> results, double totalDurationSec)
    {
        double avg = 0;
        double p95 = 0;

        var latencies = results.Select(r => r.LatencyMs).ToList();

        if (latencies.Count > 0)
        {
            avg = latencies.Average();
            var sorted = latencies.OrderBy(x => x).ToList();
            int idx = (int)Math.Ceiling(sorted.Count * 0.95) - 1;
            idx = Math.Max(0, idx);
            p95 = sorted[idx];
        }

        double throughput = totalDurationSec > 0 ? sent / totalDurationSec : 0;

        return new TestMetrics
        {
            Sent = sent,
            Success = success,
            Failure = failure,
            AverageLatencyMs = avg,
            P95LatencyMs = p95,
            Throughput = throughput,
            RequestResults = results
        };
    }
}
