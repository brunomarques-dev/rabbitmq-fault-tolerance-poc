using System;
using System.Collections.Generic;

namespace Estornos.TestRunner;

public record RequestEstorno(string IdTransacaoOriginal, decimal Valor, string Motivo);

public struct RequestResult
{
    public double StartTimeMs;
    public double LatencyMs;
    public bool IsSuccess;
}

public class TestMetrics
{
    public int Sent { get; set; }
    public int Success { get; set; }
    public int Failure { get; set; }
    public double AverageLatencyMs { get; set; }
    public double P95LatencyMs { get; set; }
    public double Throughput { get; set; }
    public List<RequestResult> RequestResults { get; set; } = new();
}

public class ScenarioResult
{
    public string Scenario { get; set; } = "";
    public string Flow { get; set; } = ""; // Síncrono / Assíncrono
    public int Sent { get; set; }
    public int Success { get; set; }
    public int Failure { get; set; }
    public double SuccessRate => Sent > 0 ? (double)Success / Sent * 100.0 : 0;
    public int Persisted { get; set; }
    public int LostData => Math.Max(0, Sent - Persisted);
    public double AverageLatencyMs { get; set; }
    public double P95LatencyMs { get; set; }
    public double Throughput { get; set; }
    public double RecoveryTimeMs { get; set; }
    public int MaxQueueDepth { get; set; }
}
