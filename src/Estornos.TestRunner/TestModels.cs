using System;
using System.Collections.Generic;

namespace Estornos.TestRunner;

public record RequestEstorno(string IdTransacaoOriginal, decimal Valor, string Motivo);

public class RequestResult
{
    public string Scenario { get; set; } = "";
    public string Flow { get; set; } = ""; // Síncrono / Assíncrono
    public int Run { get; set; }
    public string IdTransacaoOriginal { get; set; } = "";
    public DateTime StartTimestamp { get; set; }
    public DateTime EndTimestamp { get; set; }
    public double LatencyMs { get; set; }
    public int HttpStatusCode { get; set; }
    public bool IsSuccess { get; set; }
    public string? ExceptionMessage { get; set; }
}

public class QueueMetricSample
{
    public DateTime Timestamp { get; set; }
    public int MessagesReady { get; set; }
    public int MessagesUnacknowledged { get; set; }
    public int MessagesTotal { get; set; }
}

public class TestMetrics
{
    public int Sent { get; set; }
    public int HTTPAccepted { get; set; }
    public int HTTPFailed { get; set; }
    public DateTime SendStartTimestamp { get; set; }
    public DateTime SendEndTimestamp { get; set; }
    public double SendDurationMs { get; set; }
    public int ConfiguredRequestsPerSecond { get; set; }
    public int ActualSent => Sent;
    public double ActualSendRate => SendDurationMs > 0 ? (double)Sent / (SendDurationMs / 1000.0) : 0;
    public double HttpLatencyMeanAll { get; set; }
    public double HttpP95All { get; set; }
    public double HttpP99All { get; set; }
    public double HttpMaxAll { get; set; }
    public double? HttpLatencyMeanSuccess { get; set; }
    public double? HttpP95Success { get; set; }
    public double? HttpP99Success { get; set; }
    public List<RequestResult> RequestResults { get; set; } = new();
}

public class DbMetrics
{
    public int TotalPersisted { get; set; }
    public int UniquePersisted { get; set; }
    public int DuplicateRows { get; set; }
}

public class SingleRunResult
{
    public string Scenario { get; set; } = "";
    public string Flow { get; set; } = ""; // Síncrono / Assíncrono
    public int RunNumber { get; set; }

    public int Sent { get; set; }
    public int HTTPAccepted { get; set; }
    public int HTTPFailed { get; set; }
    public double HTTPAcceptanceRate => Sent > 0 ? (double)HTTPAccepted / Sent * 100.0 : 0;

    // Snapshot no término exato do envio (AtSendEnd)
    public int HTTPAcceptedAtSendEnd { get; set; }
    public int UniquePersistedAtSendEnd { get; set; }
    public int? QueueReadyAtSendEnd { get; set; }
    public int? QueueUnackedAtSendEnd { get; set; }
    public int? QueueTotalAtSendEnd { get; set; }

    // Estado final pós-drenagem (Final)
    public int PersistedRowsFinal { get; set; }
    public int UniquePersistedFinal { get; set; }
    public int DuplicateRowsFinal { get; set; }
    public int? QueueTotalFinal { get; set; }

    public int PersistedRows { get => PersistedRowsFinal; set => PersistedRowsFinal = value; }
    public int UniquePersisted { get => UniquePersistedFinal; set => UniquePersistedFinal = value; }
    public int DuplicateRows { get => DuplicateRowsFinal; set => DuplicateRowsFinal = value; }
    public int UniqueLost => Math.Max(0, Sent - UniquePersistedFinal);
    public double UniquePersistenceRate => Sent > 0 ? (double)UniquePersistedFinal / Sent * 100.0 : 0;
    public double? PreservationRate => HTTPAccepted > 0 ? (double)UniquePersistedFinal / HTTPAccepted * 100.0 : null;

    public double HttpLatencyMeanAll { get; set; }
    public double HttpP95All { get; set; }
    public double HttpP99All { get; set; }
    public double HttpMaxAll { get; set; }

    public double? HttpLatencyMeanSuccess { get; set; }
    public double? HttpP95Success { get; set; }
    public double? HttpP99Success { get; set; }

    public int ConfiguredSendRate { get; set; }
    public double ActualSendRate { get; set; }

    public int? MaxQueueDepth { get; set; }
    public int? MaxMessagesReady { get; set; }
    public int? MaxMessagesUnacked { get; set; }

    public double? HttpRecoveryTimeMs { get; set; }
    public double? PersistenceRecoveryTimeMs { get; set; }
    public double? BacklogDrainTimeMs { get; set; }

    public double SendDurationMs { get; set; }
    public double TotalExperimentDurationMs { get; set; }

    public bool? DrainCompleted { get; set; }
    public int PendingOperations { get; set; }
    public double? AverageCompletionThroughput => TotalExperimentDurationMs > 0 ? (double)UniquePersistedFinal / (TotalExperimentDurationMs / 1000.0) : null;

    public List<RequestResult> RawRequestResults { get; set; } = new();
    public List<QueueMetricSample> QueueSamples { get; set; } = new();
}

public class MetricStats
{
    public double Mean { get; set; }
    public double Min { get; set; }
    public double Max { get; set; }
    public double StdDev { get; set; }
}

public class ConsolidatedScenarioResult
{
    public string Scenario { get; set; } = "";
    public string Flow { get; set; } = "";
    public List<SingleRunResult> Runs { get; set; } = new();

    public MetricStats SentStats { get; set; } = new();
    public MetricStats HTTPAcceptedStats { get; set; } = new();
    public MetricStats HTTPFailedStats { get; set; } = new();
    public MetricStats AcceptanceRateStats { get; set; } = new();

    public MetricStats PersistedRowsStats { get; set; } = new();
    public MetricStats UniquePersistedStats { get; set; } = new();
    public MetricStats DuplicateRowsStats { get; set; } = new();
    public MetricStats UniqueLostStats { get; set; } = new();
    public MetricStats PersistenceRateStats { get; set; } = new();

    public MetricStats HttpLatencyMeanAllStats { get; set; } = new();
    public MetricStats AverageRunP95AllStats { get; set; } = new();
    public MetricStats AverageRunP99AllStats { get; set; } = new();

    public MetricStats ActualSendRateStats { get; set; } = new();
    public MetricStats MaxQueueDepthStats { get; set; } = new();

    public double GlobalP95AllRequests { get; set; }
    public double GlobalP99AllRequests { get; set; }
    public double? GlobalP95SuccessfulRequests { get; set; }
    public double? GlobalP99SuccessfulRequests { get; set; }

    public MetricStats? HttpRecoveryTimeStats { get; set; }
    public MetricStats? PersistenceRecoveryTimeStats { get; set; }
    public MetricStats? BacklogDrainTimeStats { get; set; }
    public MetricStats? CompletionThroughputStats { get; set; }
}

public class CalibrationMetricSample
{
    public DateTime Timestamp { get; set; }
    public int Sent { get; set; }
    public int HTTPAccepted { get; set; }
    public int UniquePersisted { get; set; }
    public int MessagesReady { get; set; }
    public int MessagesUnacknowledged { get; set; }
    public int MessagesTotal { get; set; }
}

public class CalibrationResult
{
    public int Rate { get; set; }
    public int Sent { get; set; }
    public int HTTPAccepted { get; set; }
    public int UniquePersistedAtSendEnd { get; set; }
    public int FinalUniquePersisted { get; set; }
    public int DuplicateRows { get; set; }
    public int MaxQueueDepth { get; set; }
    public int FinalQueueDepthAtSendEnd { get; set; }
    public double? BacklogDrainTimeMs { get; set; }
    public double ActualSendRate { get; set; }
    public string Classification { get; set; } = "";
    public double AvgBacklogGrowthPerSec { get; set; }
    public List<CalibrationMetricSample> TimeSeries { get; set; } = new();
}

