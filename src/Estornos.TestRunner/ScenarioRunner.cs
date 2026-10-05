using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Estornos.TestRunner;

public static class ScenarioRunner
{
    private static readonly string SincUrl = "http://localhost:5000/api/estornos/sinc";
    private static readonly string AsincUrl = "http://localhost:5000/api/estornos/asinc";
    public const int NumRuns = 3;

    public static async Task<List<ConsolidatedScenarioResult>> RunScenarioA(string workspacePath, int numRuns = NumRuns)
    {
        Console.WriteLine("\n=== [CENÁRIO A: FALHA DE APLICAÇÃO (5 REQ/S - 2 MIN OUTAGE WORKER)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (Estornos.Processor.Api offline por 2 min) - {numRuns} execuções...");
        var runsSinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            using var ctsSinc = new CancellationTokenSource();
            var expSw = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(
                SincUrl, 5, TimeSpan.FromSeconds(300), "Cenário A: Falha de Aplicação", "Síncrono", run, ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "estornos-processor-api");

            await Task.Delay(TimeSpan.FromSeconds(120));
            DateTime restoreTime = DateTime.UtcNow;
            DockerManager.RunDockerCommand(workspacePath, "start", "estornos-processor-api");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            DateTime sendEndTimestamp = DateTime.UtcNow;
            expSw.Stop();

            int persAtEnd = DatabaseHelper.GetEstornosCount();
            await Task.Delay(3000);
            var dbMetrics = DatabaseHelper.GetDbMetrics();

            double? httpRecoveryMs = null;
            var firstSuccess = sincMetrics.RequestResults
                .Where(r => r.IsSuccess && r.StartTimestamp >= restoreTime)
                .OrderBy(r => r.StartTimestamp)
                .FirstOrDefault();

            if (firstSuccess != null)
            {
                httpRecoveryMs = (firstSuccess.EndTimestamp - restoreTime).TotalMilliseconds;
            }

            runsSinc.Add(new SingleRunResult
            {
                Scenario = "Cenário A: Falha de Aplicação",
                Flow = "Síncrono",
                RunNumber = run,
                Sent = sincMetrics.Sent,
                HTTPAccepted = sincMetrics.HTTPAccepted,
                HTTPFailed = sincMetrics.HTTPFailed,

                // Snapshot no término do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = sincMetrics.HTTPAccepted,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = null,
                QueueUnackedAtSendEnd = null,
                QueueTotalAtSendEnd = null,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = null,

                HttpLatencyMeanAll = sincMetrics.HttpLatencyMeanAll,
                HttpP95All = sincMetrics.HttpP95All,
                HttpP99All = sincMetrics.HttpP99All,
                HttpMaxAll = sincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = sincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = sincMetrics.HttpP95Success,
                HttpP99Success = sincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = sincMetrics.ActualSendRate,
                HttpRecoveryTimeMs = httpRecoveryMs,
                PersistenceRecoveryTimeMs = httpRecoveryMs,
                BacklogDrainTimeMs = null,
                SendDurationMs = sincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = null,
                PendingOperations = 0,
                RawRequestResults = sincMetrics.RequestResults
            });
        }
        var consolidatedSinc = ReportGenerator.ConsolidateScenario("Cenário A: Falha de Aplicação", "Síncrono", runsSinc);

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (Estornos.Consumer.Worker offline por 2 min) - {numRuns} execuções...");
        var runsAsinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            var queueSamples = new List<QueueMetricSample>();
            using var ctsAsinc = new CancellationTokenSource();
            using var queueCts = RabbitMqManager.StartQueueMonitoring(ctsAsinc.Token, queueSamples, 1000);
            var expSw = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(
                AsincUrl, 5, TimeSpan.FromSeconds(300), "Cenário A: Falha de Aplicação", "Assíncrono", run, ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "estornos-consumer-worker");

            await Task.Delay(TimeSpan.FromSeconds(120));
            DateTime restoreTime = DateTime.UtcNow;
            int dbCountBeforeRestore = DatabaseHelper.GetEstornosCount();
            DockerManager.RunDockerCommand(workspacePath, "start", "estornos-consumer-worker");

            double? persistenceRecoveryMs = null;
            var recSw = Stopwatch.StartNew();
            while (recSw.Elapsed.TotalSeconds < 30)
            {
                if (DatabaseHelper.GetEstornosCount() > dbCountBeforeRestore)
                {
                    persistenceRecoveryMs = recSw.Elapsed.TotalMilliseconds;
                    break;
                }
                await Task.Delay(500);
            }

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            DateTime sendEndTimestamp = DateTime.UtcNow;

            // Snapshot no término exato do envio (AtSendEnd)
            int accAtEnd = asincMetrics.HTTPAccepted;
            int persAtEnd = DatabaseHelper.GetEstornosCount();
            var (readyAtEnd, unackedAtEnd, totalAtEnd) = await RabbitMqManager.GetQueueStatusAsync();

            // Fase de Drenagem pós-envio
            var (drainCompleted, backlogDrainMs, pendingOps, dbMetrics) = await ExecuteDrainPhaseAsync(
                asincMetrics.HTTPAccepted, sendEndTimestamp, 600);
            var (finalReady, finalUnacked, finalTotal) = await RabbitMqManager.GetQueueStatusAsync();

            expSw.Stop();
            queueCts.Cancel();

            int maxDepth = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesTotal) : totalAtEnd;
            int maxReady = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesReady) : readyAtEnd;
            int maxUnacked = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesUnacknowledged) : unackedAtEnd;

            runsAsinc.Add(new SingleRunResult
            {
                Scenario = "Cenário A: Falha de Aplicação",
                Flow = "Assíncrono",
                RunNumber = run,
                Sent = asincMetrics.Sent,
                HTTPAccepted = asincMetrics.HTTPAccepted,
                HTTPFailed = asincMetrics.HTTPFailed,

                // Snapshot no término exato do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = accAtEnd,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = readyAtEnd,
                QueueUnackedAtSendEnd = unackedAtEnd,
                QueueTotalAtSendEnd = totalAtEnd,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = finalTotal,

                HttpLatencyMeanAll = asincMetrics.HttpLatencyMeanAll,
                HttpP95All = asincMetrics.HttpP95All,
                HttpP99All = asincMetrics.HttpP99All,
                HttpMaxAll = asincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = asincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = asincMetrics.HttpP95Success,
                HttpP99Success = asincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = asincMetrics.ActualSendRate,
                MaxQueueDepth = maxDepth,
                MaxMessagesReady = maxReady,
                MaxMessagesUnacked = maxUnacked,
                HttpRecoveryTimeMs = null,
                PersistenceRecoveryTimeMs = persistenceRecoveryMs,
                BacklogDrainTimeMs = backlogDrainMs,
                SendDurationMs = asincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = drainCompleted,
                PendingOperations = pendingOps,
                RawRequestResults = asincMetrics.RequestResults,
                QueueSamples = queueSamples
            });
        }
        var consolidatedAsinc = ReportGenerator.ConsolidateScenario("Cenário A: Falha de Aplicação", "Assíncrono", runsAsinc);

        return new List<ConsolidatedScenarioResult> { consolidatedSinc, consolidatedAsinc };
    }

    public static async Task<List<ConsolidatedScenarioResult>> RunScenarioB(string workspacePath, int numRuns = NumRuns)
    {
        Console.WriteLine("\n=== [CENÁRIO B: FALHA DE BANCO DE DADOS (5 REQ/S - 2 MIN OUTAGE DB)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (SQL Server 'db' offline por 2 min) - {numRuns} execuções...");
        var runsSinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            using var ctsSinc = new CancellationTokenSource();
            var expSw = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(
                SincUrl, 5, TimeSpan.FromSeconds(300), "Cenário B: Falha de Banco de Dados", "Síncrono", run, ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "db");

            await Task.Delay(TimeSpan.FromSeconds(120));
            DateTime restoreTime = DateTime.UtcNow;
            DockerManager.RunDockerCommand(workspacePath, "start", "db");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            expSw.Stop();

            int persAtEnd = DatabaseHelper.GetEstornosCount();
            await Task.Delay(5000);
            var dbMetrics = DatabaseHelper.GetDbMetrics();

            double? httpRecoveryMs = null;
            var firstSuccess = sincMetrics.RequestResults
                .Where(r => r.IsSuccess && r.StartTimestamp >= restoreTime)
                .OrderBy(r => r.StartTimestamp)
                .FirstOrDefault();

            if (firstSuccess != null)
            {
                httpRecoveryMs = (firstSuccess.EndTimestamp - restoreTime).TotalMilliseconds;
            }

            runsSinc.Add(new SingleRunResult
            {
                Scenario = "Cenário B: Falha de Banco de Dados",
                Flow = "Síncrono",
                RunNumber = run,
                Sent = sincMetrics.Sent,
                HTTPAccepted = sincMetrics.HTTPAccepted,
                HTTPFailed = sincMetrics.HTTPFailed,

                // Snapshot no término do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = sincMetrics.HTTPAccepted,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = null,
                QueueUnackedAtSendEnd = null,
                QueueTotalAtSendEnd = null,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = null,

                HttpLatencyMeanAll = sincMetrics.HttpLatencyMeanAll,
                HttpP95All = sincMetrics.HttpP95All,
                HttpP99All = sincMetrics.HttpP99All,
                HttpMaxAll = sincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = sincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = sincMetrics.HttpP95Success,
                HttpP99Success = sincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = sincMetrics.ActualSendRate,
                HttpRecoveryTimeMs = httpRecoveryMs,
                PersistenceRecoveryTimeMs = httpRecoveryMs,
                BacklogDrainTimeMs = null,
                SendDurationMs = sincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = null,
                PendingOperations = 0,
                RawRequestResults = sincMetrics.RequestResults
            });
        }
        var consolidatedSinc = ReportGenerator.ConsolidateScenario("Cenário B: Falha de Banco de Dados", "Síncrono", runsSinc);

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (SQL Server 'db' offline por 2 min) - {numRuns} execuções...");
        var runsAsinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            var queueSamples = new List<QueueMetricSample>();
            using var ctsAsinc = new CancellationTokenSource();
            using var queueCts = RabbitMqManager.StartQueueMonitoring(ctsAsinc.Token, queueSamples, 1000);
            var expSw = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(
                AsincUrl, 5, TimeSpan.FromSeconds(300), "Cenário B: Falha de Banco de Dados", "Assíncrono", run, ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "db");

            await Task.Delay(TimeSpan.FromSeconds(120));
            DateTime restoreTime = DateTime.UtcNow;
            DockerManager.RunDockerCommand(workspacePath, "start", "db");

            double? persistenceRecoveryMs = null;
            var recSw = Stopwatch.StartNew();
            while (recSw.Elapsed.TotalSeconds < 30)
            {
                if (DatabaseHelper.GetEstornosCount() > 0)
                {
                    persistenceRecoveryMs = recSw.Elapsed.TotalMilliseconds;
                    break;
                }
                await Task.Delay(500);
            }

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            DateTime sendEndTimestamp = DateTime.UtcNow;

            // Snapshot no término exato do envio (AtSendEnd)
            int accAtEnd = asincMetrics.HTTPAccepted;
            int persAtEnd = DatabaseHelper.GetEstornosCount();
            var (readyAtEnd, unackedAtEnd, totalAtEnd) = await RabbitMqManager.GetQueueStatusAsync();

            // Fase de Drenagem pós-envio
            var (drainCompleted, backlogDrainMs, pendingOps, dbMetrics) = await ExecuteDrainPhaseAsync(
                asincMetrics.HTTPAccepted, sendEndTimestamp, 600);
            var (finalReady, finalUnacked, finalTotal) = await RabbitMqManager.GetQueueStatusAsync();

            expSw.Stop();
            queueCts.Cancel();

            int maxDepth = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesTotal) : totalAtEnd;
            int maxReady = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesReady) : readyAtEnd;
            int maxUnacked = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesUnacknowledged) : unackedAtEnd;

            runsAsinc.Add(new SingleRunResult
            {
                Scenario = "Cenário B: Falha de Banco de Dados",
                Flow = "Assíncrono",
                RunNumber = run,
                Sent = asincMetrics.Sent,
                HTTPAccepted = asincMetrics.HTTPAccepted,
                HTTPFailed = asincMetrics.HTTPFailed,

                // Snapshot no término exato do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = accAtEnd,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = readyAtEnd,
                QueueUnackedAtSendEnd = unackedAtEnd,
                QueueTotalAtSendEnd = totalAtEnd,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = finalTotal,

                HttpLatencyMeanAll = asincMetrics.HttpLatencyMeanAll,
                HttpP95All = asincMetrics.HttpP95All,
                HttpP99All = asincMetrics.HttpP99All,
                HttpMaxAll = asincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = asincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = asincMetrics.HttpP95Success,
                HttpP99Success = asincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = asincMetrics.ActualSendRate,
                MaxQueueDepth = maxDepth,
                MaxMessagesReady = maxReady,
                MaxMessagesUnacked = maxUnacked,
                HttpRecoveryTimeMs = null,
                PersistenceRecoveryTimeMs = persistenceRecoveryMs,
                BacklogDrainTimeMs = backlogDrainMs,
                SendDurationMs = asincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = drainCompleted,
                PendingOperations = pendingOps,
                RawRequestResults = asincMetrics.RequestResults,
                QueueSamples = queueSamples
            });
        }
        var consolidatedAsinc = ReportGenerator.ConsolidateScenario("Cenário B: Falha de Banco de Dados", "Assíncrono", runsAsinc);

        return new List<ConsolidatedScenarioResult> { consolidatedSinc, consolidatedAsinc };
    }

    public static async Task<List<ConsolidatedScenarioResult>> RunScenarioC(string workspacePath, int numRuns = NumRuns)
    {
        Console.WriteLine("\n=== [CENÁRIO C: OPERAÇÃO NORMAL (5 REQ/S - 5 MINUTOS CONTROLE)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (Sem interrupções) - {numRuns} execuções...");
        var runsSinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            using var ctsSinc = new CancellationTokenSource();
            var expSw = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(
                SincUrl, 5, TimeSpan.FromSeconds(300), "Cenário C: Controle", "Síncrono", run, ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            expSw.Stop();

            int persAtEnd = DatabaseHelper.GetEstornosCount();
            await Task.Delay(3000);
            var dbMetrics = DatabaseHelper.GetDbMetrics();

            runsSinc.Add(new SingleRunResult
            {
                Scenario = "Cenário C: Controle",
                Flow = "Síncrono",
                RunNumber = run,
                Sent = sincMetrics.Sent,
                HTTPAccepted = sincMetrics.HTTPAccepted,
                HTTPFailed = sincMetrics.HTTPFailed,

                // Snapshot no término do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = sincMetrics.HTTPAccepted,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = null,
                QueueUnackedAtSendEnd = null,
                QueueTotalAtSendEnd = null,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = null,

                HttpLatencyMeanAll = sincMetrics.HttpLatencyMeanAll,
                HttpP95All = sincMetrics.HttpP95All,
                HttpP99All = sincMetrics.HttpP99All,
                HttpMaxAll = sincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = sincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = sincMetrics.HttpP95Success,
                HttpP99Success = sincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = sincMetrics.ActualSendRate,
                HttpRecoveryTimeMs = null,
                PersistenceRecoveryTimeMs = null,
                BacklogDrainTimeMs = null,
                SendDurationMs = sincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = null,
                PendingOperations = 0,
                RawRequestResults = sincMetrics.RequestResults
            });
        }
        var consolidatedSinc = ReportGenerator.ConsolidateScenario("Cenário C: Controle", "Síncrono", runsSinc);

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (Sem interrupções) - {numRuns} execuções...");
        var runsAsinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            var queueSamples = new List<QueueMetricSample>();
            using var ctsAsinc = new CancellationTokenSource();
            using var queueCts = RabbitMqManager.StartQueueMonitoring(ctsAsinc.Token, queueSamples, 1000);
            var expSw = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(
                AsincUrl, 5, TimeSpan.FromSeconds(300), "Cenário C: Controle", "Assíncrono", run, ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            DateTime sendEndTimestamp = DateTime.UtcNow;

            // Snapshot no término exato do envio (AtSendEnd)
            int accAtEnd = asincMetrics.HTTPAccepted;
            int persAtEnd = DatabaseHelper.GetEstornosCount();
            var (readyAtEnd, unackedAtEnd, totalAtEnd) = await RabbitMqManager.GetQueueStatusAsync();

            // Fase de Drenagem pós-envio
            var (drainCompleted, backlogDrainMs, pendingOps, dbMetrics) = await ExecuteDrainPhaseAsync(
                asincMetrics.HTTPAccepted, sendEndTimestamp, 600);
            var (finalReady, finalUnacked, finalTotal) = await RabbitMqManager.GetQueueStatusAsync();

            expSw.Stop();
            queueCts.Cancel();

            int maxDepth = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesTotal) : totalAtEnd;
            int maxReady = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesReady) : readyAtEnd;
            int maxUnacked = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesUnacknowledged) : unackedAtEnd;

            runsAsinc.Add(new SingleRunResult
            {
                Scenario = "Cenário C: Controle",
                Flow = "Assíncrono",
                RunNumber = run,
                Sent = asincMetrics.Sent,
                HTTPAccepted = asincMetrics.HTTPAccepted,
                HTTPFailed = asincMetrics.HTTPFailed,

                // Snapshot no término exato do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = accAtEnd,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = readyAtEnd,
                QueueUnackedAtSendEnd = unackedAtEnd,
                QueueTotalAtSendEnd = totalAtEnd,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = finalTotal,

                HttpLatencyMeanAll = asincMetrics.HttpLatencyMeanAll,
                HttpP95All = asincMetrics.HttpP95All,
                HttpP99All = asincMetrics.HttpP99All,
                HttpMaxAll = asincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = asincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = asincMetrics.HttpP95Success,
                HttpP99Success = asincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = asincMetrics.ActualSendRate,
                MaxQueueDepth = maxDepth,
                MaxMessagesReady = maxReady,
                MaxMessagesUnacked = maxUnacked,
                HttpRecoveryTimeMs = null,
                PersistenceRecoveryTimeMs = null,
                BacklogDrainTimeMs = backlogDrainMs,
                SendDurationMs = asincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = drainCompleted,
                PendingOperations = pendingOps,
                RawRequestResults = asincMetrics.RequestResults,
                QueueSamples = queueSamples
            });
        }
        var consolidatedAsinc = ReportGenerator.ConsolidateScenario("Cenário C: Controle", "Assíncrono", runsAsinc);

        return new List<ConsolidatedScenarioResult> { consolidatedSinc, consolidatedAsinc };
    }

    public static async Task<List<ConsolidatedScenarioResult>> RunScenarioD(string workspacePath, int numRuns = NumRuns)
    {
        Console.WriteLine("\n=== [CENÁRIO D: TESTE DE CARGA (150 REQ/S - 5 MINUTOS - 45.000 TARGET)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (Carga de 150 req/s) - {numRuns} execuções...");
        var runsSinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            using var ctsSinc = new CancellationTokenSource();
            var expSw = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(
                SincUrl, 150, TimeSpan.FromSeconds(300), "Cenário D: Teste de Carga", "Síncrono", run, ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            expSw.Stop();

            int persAtEnd = DatabaseHelper.GetEstornosCount();
            await Task.Delay(5000);
            var dbMetrics = DatabaseHelper.GetDbMetrics();

            runsSinc.Add(new SingleRunResult
            {
                Scenario = "Cenário D: Teste de Carga",
                Flow = "Síncrono",
                RunNumber = run,
                Sent = sincMetrics.Sent,
                HTTPAccepted = sincMetrics.HTTPAccepted,
                HTTPFailed = sincMetrics.HTTPFailed,

                // Snapshot no término do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = sincMetrics.HTTPAccepted,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = null,
                QueueUnackedAtSendEnd = null,
                QueueTotalAtSendEnd = null,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = null,

                HttpLatencyMeanAll = sincMetrics.HttpLatencyMeanAll,
                HttpP95All = sincMetrics.HttpP95All,
                HttpP99All = sincMetrics.HttpP99All,
                HttpMaxAll = sincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = sincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = sincMetrics.HttpP95Success,
                HttpP99Success = sincMetrics.HttpP99Success,
                ConfiguredSendRate = 150,
                ActualSendRate = sincMetrics.ActualSendRate,
                HttpRecoveryTimeMs = null,
                PersistenceRecoveryTimeMs = null,
                BacklogDrainTimeMs = null,
                SendDurationMs = sincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = null,
                PendingOperations = 0,
                RawRequestResults = sincMetrics.RequestResults
            });
        }
        var consolidatedSinc = ReportGenerator.ConsolidateScenario("Cenário D: Teste de Carga", "Síncrono", runsSinc);

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (Carga de 150 req/s) - {numRuns} execuções...");
        var runsAsinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            var queueSamples = new List<QueueMetricSample>();
            using var ctsAsinc = new CancellationTokenSource();
            using var queueCts = RabbitMqManager.StartQueueMonitoring(ctsAsinc.Token, queueSamples, 1000);
            var expSw = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(
                AsincUrl, 150, TimeSpan.FromSeconds(300), "Cenário D: Teste de Carga", "Assíncrono", run, ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            DateTime sendEndTimestamp = DateTime.UtcNow;

            // Snapshot no término exato do envio (AtSendEnd)
            int accAtEnd = asincMetrics.HTTPAccepted;
            int persAtEnd = DatabaseHelper.GetEstornosCount();
            var (readyAtEnd, unackedAtEnd, totalAtEnd) = await RabbitMqManager.GetQueueStatusAsync();

            Console.WriteLine("[CENÁRIO D] Envio de 300s encerrado. Iniciando medição do BacklogDrainTime pós-envio (Timeout de Segurança = 600s)...");

            var (drainCompleted, backlogDrainMs, pendingOps, dbMetrics) = await ExecuteDrainPhaseAsync(
                asincMetrics.HTTPAccepted, sendEndTimestamp, 600);
            var (finalReady, finalUnacked, finalTotal) = await RabbitMqManager.GetQueueStatusAsync();

            expSw.Stop();
            queueCts.Cancel();

            int maxDepth = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesTotal) : totalAtEnd;
            int maxReady = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesReady) : readyAtEnd;
            int maxUnacked = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesUnacknowledged) : unackedAtEnd;

            runsAsinc.Add(new SingleRunResult
            {
                Scenario = "Cenário D: Teste de Carga",
                Flow = "Assíncrono",
                RunNumber = run,
                Sent = asincMetrics.Sent,
                HTTPAccepted = asincMetrics.HTTPAccepted,
                HTTPFailed = asincMetrics.HTTPFailed,

                // Snapshot no término exato do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = accAtEnd,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = readyAtEnd,
                QueueUnackedAtSendEnd = unackedAtEnd,
                QueueTotalAtSendEnd = totalAtEnd,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = finalTotal,

                HttpLatencyMeanAll = asincMetrics.HttpLatencyMeanAll,
                HttpP95All = asincMetrics.HttpP95All,
                HttpP99All = asincMetrics.HttpP99All,
                HttpMaxAll = asincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = asincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = asincMetrics.HttpP95Success,
                HttpP99Success = asincMetrics.HttpP99Success,
                ConfiguredSendRate = 150,
                ActualSendRate = asincMetrics.ActualSendRate,
                MaxQueueDepth = maxDepth,
                MaxMessagesReady = maxReady,
                MaxMessagesUnacked = maxUnacked,
                HttpRecoveryTimeMs = null,
                PersistenceRecoveryTimeMs = null,
                BacklogDrainTimeMs = backlogDrainMs,
                SendDurationMs = asincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = drainCompleted,
                PendingOperations = pendingOps,
                RawRequestResults = asincMetrics.RequestResults,
                QueueSamples = queueSamples
            });
        }
        var consolidatedAsinc = ReportGenerator.ConsolidateScenario("Cenário D: Teste de Carga", "Assíncrono", runsAsinc);

        return new List<ConsolidatedScenarioResult> { consolidatedSinc, consolidatedAsinc };
    }

    public static async Task<List<ConsolidatedScenarioResult>> RunScenarioE(string workspacePath, int numRuns = NumRuns)
    {
        Console.WriteLine("\n=== [CENÁRIO E: FALHA DE MENSAGERIA (5 REQ/S - 2 MIN OUTAGE RABBITMQ)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (RabbitMQ 'rabbitmq' offline por 2 min) - {numRuns} execuções...");
        var runsSinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            using var ctsSinc = new CancellationTokenSource();
            var expSw = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(
                SincUrl, 5, TimeSpan.FromSeconds(300), "Cenário E: Falha de Mensageria", "Síncrono", run, ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "rabbitmq");

            await Task.Delay(TimeSpan.FromSeconds(120));
            DockerManager.RunDockerCommand(workspacePath, "start", "rabbitmq");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            expSw.Stop();

            int persAtEnd = DatabaseHelper.GetEstornosCount();
            await Task.Delay(3000);
            var dbMetrics = DatabaseHelper.GetDbMetrics();

            runsSinc.Add(new SingleRunResult
            {
                Scenario = "Cenário E: Falha de Mensageria",
                Flow = "Síncrono",
                RunNumber = run,
                Sent = sincMetrics.Sent,
                HTTPAccepted = sincMetrics.HTTPAccepted,
                HTTPFailed = sincMetrics.HTTPFailed,

                // Snapshot no término do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = sincMetrics.HTTPAccepted,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = null,
                QueueUnackedAtSendEnd = null,
                QueueTotalAtSendEnd = null,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = null,

                HttpLatencyMeanAll = sincMetrics.HttpLatencyMeanAll,
                HttpP95All = sincMetrics.HttpP95All,
                HttpP99All = sincMetrics.HttpP99All,
                HttpMaxAll = sincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = sincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = sincMetrics.HttpP95Success,
                HttpP99Success = sincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = sincMetrics.ActualSendRate,
                HttpRecoveryTimeMs = null,
                PersistenceRecoveryTimeMs = null,
                BacklogDrainTimeMs = null,
                SendDurationMs = sincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = null,
                PendingOperations = 0,
                RawRequestResults = sincMetrics.RequestResults
            });
        }
        var consolidatedSinc = ReportGenerator.ConsolidateScenario("Cenário E: Falha de Mensageria", "Síncrono", runsSinc);

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (RabbitMQ 'rabbitmq' offline por 2 min) - {numRuns} execuções...");
        var runsAsinc = new List<SingleRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{numRuns}...");
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            var queueSamples = new List<QueueMetricSample>();
            using var ctsAsinc = new CancellationTokenSource();
            using var queueCts = RabbitMqManager.StartQueueMonitoring(ctsAsinc.Token, queueSamples, 1000);
            var expSw = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(
                AsincUrl, 5, TimeSpan.FromSeconds(300), "Cenário E: Falha de Mensageria", "Assíncrono", run, ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "rabbitmq");

            await Task.Delay(TimeSpan.FromSeconds(120));
            DateTime restoreTime = DateTime.UtcNow;
            DockerManager.RunDockerCommand(workspacePath, "start", "rabbitmq");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            DateTime sendEndTimestamp = DateTime.UtcNow;

            // Snapshot no término exato do envio (AtSendEnd)
            int accAtEnd = asincMetrics.HTTPAccepted;
            int persAtEnd = DatabaseHelper.GetEstornosCount();
            var (readyAtEnd, unackedAtEnd, totalAtEnd) = await RabbitMqManager.GetQueueStatusAsync();

            double? httpRecoveryMs = null;
            var firstSuccess = asincMetrics.RequestResults
                .Where(r => r.IsSuccess && r.StartTimestamp >= restoreTime)
                .OrderBy(r => r.StartTimestamp)
                .FirstOrDefault();

            if (firstSuccess != null)
            {
                httpRecoveryMs = (firstSuccess.EndTimestamp - restoreTime).TotalMilliseconds;
            }

            var (drainCompleted, backlogDrainMs, pendingOps, dbMetrics) = await ExecuteDrainPhaseAsync(
                asincMetrics.HTTPAccepted, restoreTime, 600);
            var (finalReady, finalUnacked, finalTotal) = await RabbitMqManager.GetQueueStatusAsync();

            expSw.Stop();
            queueCts.Cancel();

            int maxDepth = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesTotal) : totalAtEnd;
            int maxReady = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesReady) : readyAtEnd;
            int maxUnacked = queueSamples.Count > 0 ? queueSamples.Max(s => s.MessagesUnacknowledged) : unackedAtEnd;

            runsAsinc.Add(new SingleRunResult
            {
                Scenario = "Cenário E: Falha de Mensageria",
                Flow = "Assíncrono",
                RunNumber = run,
                Sent = asincMetrics.Sent,
                HTTPAccepted = asincMetrics.HTTPAccepted,
                HTTPFailed = asincMetrics.HTTPFailed,

                // Snapshot no término exato do envio (AtSendEnd)
                HTTPAcceptedAtSendEnd = accAtEnd,
                UniquePersistedAtSendEnd = persAtEnd,
                QueueReadyAtSendEnd = readyAtEnd,
                QueueUnackedAtSendEnd = unackedAtEnd,
                QueueTotalAtSendEnd = totalAtEnd,

                // Estado final pós-drenagem (Final)
                PersistedRowsFinal = dbMetrics.TotalPersisted,
                UniquePersistedFinal = dbMetrics.UniquePersisted,
                DuplicateRowsFinal = dbMetrics.DuplicateRows,
                QueueTotalFinal = finalTotal,

                HttpLatencyMeanAll = asincMetrics.HttpLatencyMeanAll,
                HttpP95All = asincMetrics.HttpP95All,
                HttpP99All = asincMetrics.HttpP99All,
                HttpMaxAll = asincMetrics.HttpMaxAll,
                HttpLatencyMeanSuccess = asincMetrics.HttpLatencyMeanSuccess,
                HttpP95Success = asincMetrics.HttpP95Success,
                HttpP99Success = asincMetrics.HttpP99Success,
                ConfiguredSendRate = 5,
                ActualSendRate = asincMetrics.ActualSendRate,
                MaxQueueDepth = maxDepth,
                MaxMessagesReady = maxReady,
                MaxMessagesUnacked = maxUnacked,
                HttpRecoveryTimeMs = httpRecoveryMs,
                PersistenceRecoveryTimeMs = httpRecoveryMs,
                BacklogDrainTimeMs = backlogDrainMs,
                SendDurationMs = asincMetrics.SendDurationMs,
                TotalExperimentDurationMs = expSw.Elapsed.TotalMilliseconds,
                DrainCompleted = drainCompleted,
                PendingOperations = pendingOps,
                RawRequestResults = asincMetrics.RequestResults,
                QueueSamples = queueSamples
            });
        }
        var consolidatedAsinc = ReportGenerator.ConsolidateScenario("Cenário E: Falha de Mensageria", "Assíncrono", runsAsinc);

        return new List<ConsolidatedScenarioResult> { consolidatedSinc, consolidatedAsinc };
    }

    private static async Task<(bool drainCompleted, double backlogDrainMs, int pendingOperations, DbMetrics dbMetrics)> ExecuteDrainPhaseAsync(
        int targetAcceptedCount, DateTime referenceStartTimestamp, int maxTimeoutSeconds = 600)
    {
        var drainSw = Stopwatch.StartNew();
        bool drainCompleted = false;
        DbMetrics dbMetrics = new();
        int consecutiveZeroReadings = 0;

        while (drainSw.Elapsed.TotalSeconds < maxTimeoutSeconds)
        {
            dbMetrics = DatabaseHelper.GetDbMetrics();
            var (ready, unacked, total) = await RabbitMqManager.GetQueueStatusAsync();

            if (dbMetrics.UniquePersisted >= targetAcceptedCount && total == 0)
            {
                consecutiveZeroReadings++;
                if (consecutiveZeroReadings >= 2)
                {
                    drainCompleted = true;
                    break;
                }
            }
            else
            {
                consecutiveZeroReadings = 0;
            }

            await Task.Delay(1000);
        }

        drainSw.Stop();
        dbMetrics = DatabaseHelper.GetDbMetrics();

        double backlogDrainMs = (DateTime.UtcNow - referenceStartTimestamp).TotalMilliseconds;
        int pendingOps = Math.Max(0, targetAcceptedCount - dbMetrics.UniquePersisted);

        return (drainCompleted, backlogDrainMs, pendingOps, dbMetrics);
    }

    public static async Task<List<CalibrationResult>> RunRateCalibrationAsync(string workspacePath)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("\n========================================================================");
        Console.WriteLine("    CALIBRAÇÃO DE TAXA-BASE (3, 5 E 7 REQ/S POR 120 SEGUNDOS)");
        Console.WriteLine("========================================================================");
        Console.ResetColor();

        int[] rates = new int[] { 3, 5, 7 };
        var calibrationResults = new List<CalibrationResult>();

        foreach (int rate in rates)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"\n------------------------------------------------------------------------");
            Console.WriteLine($" [TESTE DE CALIBRAÇÃO] Taxa: {rate} req/s | Duração: 120s | Fluxo Assíncrono");
            Console.WriteLine($"------------------------------------------------------------------------");
            Console.ResetColor();

            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);
            await DockerManager.RunWarmUpAsync(workspacePath);
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            var checkDb = DatabaseHelper.GetDbMetrics();
            var (checkReady, checkUnacked, checkTotal) = await RabbitMqManager.GetQueueStatusAsync();

            Console.WriteLine($"[CONFIRMAÇÃO PRÉ-TESTE] Banco = {checkDb.TotalPersisted} | Fila RabbitMQ = {checkTotal}");
            if (checkDb.TotalPersisted != 0 || checkTotal != 0)
            {
                throw new Exception($"Ambiente não está limpo pré-teste {rate} req/s! Banco: {checkDb.TotalPersisted}, Fila: {checkTotal}");
            }

            using var loadCts = new CancellationTokenSource();
            var timeSeries = new List<CalibrationMetricSample>();

            var expSw = Stopwatch.StartNew();

            var loadTask = BenchmarkEngine.SendConstantRateLoadAsync(
                AsincUrl, rate, TimeSpan.FromSeconds(120), "Calibração Taxa-Base", "Assíncrono", rate, loadCts.Token);

            var monitorTask = Task.Run(async () =>
            {
                while (!loadCts.Token.IsCancellationRequested)
                {
                    var (ready, unacked, total) = await RabbitMqManager.GetQueueStatusAsync();
                    int persisted = DatabaseHelper.GetEstornosCount();
                    var (sentNow, accNow) = BenchmarkEngine.CurrentProgress;

                    timeSeries.Add(new CalibrationMetricSample
                    {
                        Timestamp = DateTime.UtcNow,
                        Sent = sentNow,
                        HTTPAccepted = accNow,
                        UniquePersisted = persisted,
                        MessagesReady = ready,
                        MessagesUnacknowledged = unacked,
                        MessagesTotal = total
                    });

                    Console.WriteLine($"  [t={timeSeries.Count}s] Sent={sentNow} | Accepted={accNow} | Persisted={persisted} | Ready={ready} | Unacked={unacked} | TotalQueue={total}");

                    try
                    {
                        await Task.Delay(1000, loadCts.Token);
                    }
                    catch (TaskCanceledException) { break; }
                }
            });

            var loadMetrics = await loadTask;
            DateTime sendEndTimestamp = DateTime.UtcNow;

            loadCts.Cancel();
            try { await monitorTask; } catch { }

            var (finalReadyEnd, finalUnackedEnd, finalTotalEnd) = await RabbitMqManager.GetQueueStatusAsync();
            int persistedAtSendEnd = DatabaseHelper.GetEstornosCount();

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"\n[TÉRMINO DO ENVIO - 120s ({rate} req/s)]");
            Console.WriteLine($"  Sent: {loadMetrics.Sent} | HTTPAccepted: {loadMetrics.HTTPAccepted}");
            Console.WriteLine($"  Persistidas ao fim do envio: {persistedAtSendEnd}");
            Console.WriteLine($"  Profundidade da Fila ao fim do envio: {finalTotalEnd} (Ready: {finalReadyEnd}, Unacked: {finalUnackedEnd})");
            Console.ResetColor();

            var (drainCompleted, backlogDrainMs, pendingOps, dbMetrics) = await ExecuteDrainPhaseAsync(
                loadMetrics.HTTPAccepted, sendEndTimestamp, 300);

            expSw.Stop();

            int maxQueueDepth = timeSeries.Count > 0 ? timeSeries.Max(s => s.MessagesTotal) : finalTotalEnd;
            if (finalTotalEnd > maxQueueDepth) maxQueueDepth = finalTotalEnd;

            double avgGrowthPerSec = 0;
            if (timeSeries.Count > 1)
            {
                avgGrowthPerSec = (double)(finalTotalEnd - timeSeries.First().MessagesTotal) / timeSeries.Count;
            }

            string classification;
            if (finalTotalEnd <= 5 && maxQueueDepth <= 15 && avgGrowthPerSec < 0.05)
            {
                classification = "ESTÁVEL";
            }
            else if (finalTotalEnd <= 30 && avgGrowthPerSec < 0.2)
            {
                classification = "LIMÍTROFE";
            }
            else
            {
                classification = "INSUSTENTÁVEL";
            }

            double? actualDrainTime = finalTotalEnd == 0 ? 0 : backlogDrainMs;

            var calResult = new CalibrationResult
            {
                Rate = rate,
                Sent = loadMetrics.Sent,
                HTTPAccepted = loadMetrics.HTTPAccepted,
                UniquePersistedAtSendEnd = persistedAtSendEnd,
                FinalUniquePersisted = dbMetrics.UniquePersisted,
                DuplicateRows = dbMetrics.DuplicateRows,
                MaxQueueDepth = maxQueueDepth,
                FinalQueueDepthAtSendEnd = finalTotalEnd,
                BacklogDrainTimeMs = actualDrainTime,
                ActualSendRate = loadMetrics.ActualSendRate,
                Classification = classification,
                AvgBacklogGrowthPerSec = avgGrowthPerSec,
                TimeSeries = timeSeries
            };

            calibrationResults.Add(calResult);
            SaveCalibrationCsv(workspacePath, calResult);
        }

        PrintCalibrationSummaryTable(calibrationResults);
        return calibrationResults;
    }

    private static void SaveCalibrationCsv(string workspacePath, CalibrationResult result)
    {
        try
        {
            string calDir = Path.Combine(workspacePath, "results", "calibration");
            Directory.CreateDirectory(calDir);

            string file = Path.Combine(calDir, $"calibração-{result.Rate}reqs.csv");
            using var writer = new StreamWriter(file, false, System.Text.Encoding.UTF8);
            writer.WriteLine("Timestamp,Sent,HTTPAccepted,UniquePersisted,MessagesReady,MessagesUnacknowledged,MessagesTotal");
            foreach (var s in result.TimeSeries)
            {
                writer.WriteLine($"{s.Timestamp:yyyy-MM-dd HH:mm:ss.fff},{s.Sent},{s.HTTPAccepted},{s.UniquePersisted},{s.MessagesReady},{s.MessagesUnacknowledged},{s.MessagesTotal}");
            }
            Console.WriteLine($"[SÉRIE TEMPORAL SALVA] {file}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO SALVAR CALIBRAÇÃO CSV] {ex.Message}");
        }
    }

    private static void PrintCalibrationSummaryTable(List<CalibrationResult> results)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("\n========================================================================");
        Console.WriteLine("          RESULTADO DA CALIBRAÇÃO DA TAXA-BASE ");
        Console.WriteLine("========================================================================");
        Console.ResetColor();

        Console.WriteLine("| Taxa | Enviadas | Persistidas ao fim do envio | Max fila | Fila ao fim do envio | Drenagem | Classificação | Crescimento Fila/s |");
        Console.WriteLine("|------|----------|------------------------------|----------|----------------------|----------|---------------|-------------------|");

        foreach (var r in results)
        {
            string drainStr = r.BacklogDrainTimeMs.HasValue && r.FinalQueueDepthAtSendEnd > 0 ? $"{r.BacklogDrainTimeMs.Value:F1} ms" : "0 ms (Zerada)";
            Console.WriteLine($"| {r.Rate} req/s | {r.Sent} | {r.UniquePersistedAtSendEnd} | {r.MaxQueueDepth} | {r.FinalQueueDepthAtSendEnd} | {drainStr} | {r.Classification} | +{r.AvgBacklogGrowthPerSec:F2} msgs/s |");
        }
        Console.WriteLine("========================================================================\n");
    }
}
