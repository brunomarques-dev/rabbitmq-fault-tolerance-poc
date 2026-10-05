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
    private static int _currentSent = 0;
    private static int _currentAccepted = 0;

    public static (int Sent, int HTTPAccepted) CurrentProgress => (_currentSent, _currentAccepted);

    private static readonly HttpClient HttpClientInstance = new(new SocketsHttpHandler
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(15),
        MaxConnectionsPerServer = 200
    })
    {
        Timeout = TimeSpan.FromSeconds(60) // Timeout HTTP de 60s mantido sem alterações
    };

    public static async Task<TestMetrics> SendConstantRateLoadAsync(
        string url,
        int ratePerSecond,
        TimeSpan duration,
        string scenarioName,
        string flowName,
        int runNumber,
        CancellationToken token)
    {
        _currentSent = 0;
        _currentAccepted = 0;

        var resultsBag = new ConcurrentBag<RequestResult>();
        int sent = 0;
        int httpAccepted = 0;
        int httpFailed = 0;


        DateTime sendStartTimestamp = DateTime.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        var tasks = new List<Task>();
        double intervalMs = 1000.0 / ratePerSecond;

        long nextSendTicks = stopwatch.ElapsedTicks;
        double ticksPerMs = Stopwatch.Frequency / 1000.0;

        int totalTargetRequests = (int)Math.Round(ratePerSecond * duration.TotalSeconds);
        int requestId = 0;

        while (requestId < totalTargetRequests && !token.IsCancellationRequested)
        {
            int currentSentVal = Interlocked.Increment(ref sent);
            Interlocked.Exchange(ref _currentSent, currentSentVal);
            int currentId = requestId++;

            tasks.Add(Task.Run(async () =>
            {
                var reqStartTimestamp = DateTime.UtcNow;
                var reqStopwatch = Stopwatch.StartNew();
                bool isSuccess = false;
                int statusCode = 0;
                string? exceptionMsg = null;
                string txId = $"TX-{Guid.NewGuid()}";

                using var reqCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

                try
                {
                    var payload = new RequestEstorno(txId, 50.00m, $"Teste Carga {currentId}");
                    var json = JsonSerializer.Serialize(payload);
                    using var content = new StringContent(json, Encoding.UTF8, "application/json");

                    var response = await HttpClientInstance.PostAsync(url, content, reqCts.Token);
                    reqStopwatch.Stop();
                    statusCode = (int)response.StatusCode;
                    isSuccess = response.IsSuccessStatusCode;

                    if (isSuccess)
                    {
                        int currentAccVal = Interlocked.Increment(ref httpAccepted);
                        Interlocked.Exchange(ref _currentAccepted, currentAccVal);
                    }
                    else Interlocked.Increment(ref httpFailed);
                }
                catch (Exception ex)
                {
                    reqStopwatch.Stop();
                    exceptionMsg = ex.Message;
                    Interlocked.Increment(ref httpFailed);
                }
                finally
                {
                    var reqEndTimestamp = DateTime.UtcNow;
                    double latency = reqStopwatch.Elapsed.TotalMilliseconds;
                    resultsBag.Add(new RequestResult
                    {
                        Scenario = scenarioName,
                        Flow = flowName,
                        Run = runNumber,
                        IdTransacaoOriginal = txId,
                        StartTimestamp = reqStartTimestamp,
                        EndTimestamp = reqEndTimestamp,
                        LatencyMs = latency,
                        HttpStatusCode = statusCode,
                        IsSuccess = isSuccess,
                        ExceptionMessage = exceptionMsg
                    });
                }
            }));

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
        DateTime sendEndTimestamp = DateTime.UtcNow;
        double sendDurationMs = stopwatch.Elapsed.TotalMilliseconds;

        return CalculateMetrics(
            sent,
            httpAccepted,
            httpFailed,
            resultsBag.ToList(),
            sendStartTimestamp,
            sendEndTimestamp,
            sendDurationMs,
            ratePerSecond);
    }

    private static TestMetrics CalculateMetrics(
        int sent,
        int httpAccepted,
        int httpFailed,
        List<RequestResult> results,
        DateTime sendStartTimestamp,
        DateTime sendEndTimestamp,
        double sendDurationMs,
        int ratePerSecond)
    {
        double meanAll = 0, p95All = 0, p99All = 0, maxAll = 0;
        double? meanSuccess = null, p95Success = null, p99Success = null;

        var allLatencies = results.Select(r => r.LatencyMs).OrderBy(x => x).ToList();
        if (allLatencies.Count > 0)
        {
            meanAll = allLatencies.Average();
            maxAll = allLatencies.Max();
            p95All = GetPercentile(allLatencies, 0.95);
            p99All = GetPercentile(allLatencies, 0.99);
        }

        var successLatencies = results.Where(r => r.IsSuccess).Select(r => r.LatencyMs).OrderBy(x => x).ToList();
        if (successLatencies.Count > 0)
        {
            meanSuccess = successLatencies.Average();
            p95Success = GetPercentile(successLatencies, 0.95);
            p99Success = GetPercentile(successLatencies, 0.99);
        }

        return new TestMetrics
        {
            Sent = sent,
            HTTPAccepted = httpAccepted,
            HTTPFailed = httpFailed,
            SendStartTimestamp = sendStartTimestamp,
            SendEndTimestamp = sendEndTimestamp,
            SendDurationMs = sendDurationMs,
            ConfiguredRequestsPerSecond = ratePerSecond,
            HttpLatencyMeanAll = meanAll,
            HttpP95All = p95All,
            HttpP99All = p99All,
            HttpMaxAll = maxAll,
            HttpLatencyMeanSuccess = meanSuccess,
            HttpP95Success = p95Success,
            HttpP99Success = p99Success,
            RequestResults = results
        };
    }

    public static double GetPercentile(List<double> sortedValues, double percentile)
    {
        if (sortedValues == null || sortedValues.Count == 0) return 0;
        int idx = (int)Math.Ceiling(sortedValues.Count * percentile) - 1;
        idx = Math.Clamp(idx, 0, sortedValues.Count - 1);
        return sortedValues[idx];
    }
}
