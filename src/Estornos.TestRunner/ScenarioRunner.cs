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
    private const int NumRuns = 1;

    public static async Task RunScenarioA(string workspacePath, List<ScenarioResult> results)
    {
        Console.WriteLine("\n=== [CENÁRIO A: FALHA DE APLICAÇÃO (10 REQ/S - 2 MIN OUTAGE)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (Estornos.Processor.Api offline por 2 min) - {NumRuns} execuções...");
        var runsSinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsSinc = new CancellationTokenSource();
            var stopwatchSinc = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(SincUrl, 10, TimeSpan.FromSeconds(300), ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "estornos-processor-api");

            await Task.Delay(TimeSpan.FromSeconds(120));
            long startServiceTimeMs = stopwatchSinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "start", "estornos-processor-api");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            stopwatchSinc.Stop();

            double recoveryTimeMs = 0;
            var firstSuccess = sincMetrics.RequestResults
                .Where(r => r.IsSuccess && r.StartTimeMs >= startServiceTimeMs)
                .OrderBy(r => r.StartTimeMs)
                .Cast<RequestResult?>()
                .FirstOrDefault();

            if (firstSuccess.HasValue)
            {
                recoveryTimeMs = (firstSuccess.Value.StartTimeMs + firstSuccess.Value.LatencyMs) - startServiceTimeMs;
            }

            await Task.Delay(3000);
            int sincPersisted = DatabaseHelper.GetEstornosCount();

            runsSinc.Add(new ScenarioResult
            {
                Scenario = "Cenário A: Falha de Aplicação",
                Flow = "Síncrono",
                Sent = sincMetrics.Sent,
                Success = sincMetrics.Success,
                Failure = sincMetrics.Failure,
                Persisted = sincPersisted,
                AverageLatencyMs = sincMetrics.AverageLatencyMs,
                P95LatencyMs = sincMetrics.P95LatencyMs,
                Throughput = sincMetrics.Throughput,
                RecoveryTimeMs = recoveryTimeMs,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário A: Falha de Aplicação", "Síncrono", runsSinc));

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (Estornos.Consumer.Worker offline por 2 min) - {NumRuns} execuções...");
        var runsAsinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsAsinc = new CancellationTokenSource();
            var stopwatchAsinc = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(AsincUrl, 10, TimeSpan.FromSeconds(300), ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            long stopWorkerTimeMs = stopwatchAsinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "stop", "estornos-consumer-worker");

            await Task.Delay(TimeSpan.FromSeconds(120));
            long startWorkerTimeMs = stopwatchAsinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "start", "estornos-consumer-worker");

            double workerRecoveryMs = 0;
            var recSw = Stopwatch.StartNew();
            int initialCount = DatabaseHelper.GetEstornosCount();
            while (recSw.Elapsed.TotalSeconds < 30)
            {
                if (DatabaseHelper.GetEstornosCount() > initialCount)
                {
                    workerRecoveryMs = recSw.Elapsed.TotalMilliseconds;
                    break;
                }
                await Task.Delay(50);
            }

            Console.WriteLine("Aguardando fila esvaziar (sincronização no banco)...");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            stopwatchAsinc.Stop();

            await WaitForQueueToDrainAsync(asincMetrics.Success);

            int maxBacklog = asincMetrics.RequestResults
                .Count(r => r.IsSuccess && r.StartTimeMs >= stopWorkerTimeMs && r.StartTimeMs <= startWorkerTimeMs);

            runsAsinc.Add(new ScenarioResult
            {
                Scenario = "Cenário A: Falha de Aplicação",
                Flow = "Assíncrono",
                Sent = asincMetrics.Sent,
                Success = asincMetrics.Success,
                Failure = asincMetrics.Failure,
                Persisted = DatabaseHelper.GetEstornosCount(),
                AverageLatencyMs = asincMetrics.AverageLatencyMs,
                P95LatencyMs = asincMetrics.P95LatencyMs,
                Throughput = asincMetrics.Throughput,
                RecoveryTimeMs = workerRecoveryMs,
                MaxQueueDepth = maxBacklog
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário A: Falha de Aplicação", "Assíncrono", runsAsinc));
    }

    public static async Task RunScenarioB(string workspacePath, List<ScenarioResult> results)
    {
        Console.WriteLine("\n=== [CENÁRIO B: FALHA DE BANCO DE DADOS (10 REQ/S - 2 MIN OUTAGE)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (SQL Server 'db' offline por 2 min) - {NumRuns} execuções...");
        var runsSinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsSinc = new CancellationTokenSource();
            var stopwatchSinc = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(SincUrl, 10, TimeSpan.FromSeconds(300), ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "db");

            await Task.Delay(TimeSpan.FromSeconds(120));
            long startServiceTimeMs = stopwatchSinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "start", "db");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            stopwatchSinc.Stop();

            double recoveryTimeMs = 0;
            var firstSuccess = sincMetrics.RequestResults
                .Where(r => r.IsSuccess && r.StartTimeMs >= startServiceTimeMs)
                .OrderBy(r => r.StartTimeMs)
                .Cast<RequestResult?>()
                .FirstOrDefault();

            if (firstSuccess.HasValue)
            {
                recoveryTimeMs = (firstSuccess.Value.StartTimeMs + firstSuccess.Value.LatencyMs) - startServiceTimeMs;
            }

            Console.WriteLine("Aguardando banco de dados responder para contar registros...");
            int sincPersisted = 0;
            var retrySw = Stopwatch.StartNew();
            while (retrySw.Elapsed.TotalSeconds < 30)
            {
                try
                {
                    sincPersisted = DatabaseHelper.GetEstornosCount();
                    break;
                }
                catch
                {
                    await Task.Delay(1000);
                }
            }

            runsSinc.Add(new ScenarioResult
            {
                Scenario = "Cenário B: Falha de Banco de Dados",
                Flow = "Síncrono",
                Sent = sincMetrics.Sent,
                Success = sincMetrics.Success,
                Failure = sincMetrics.Failure,
                Persisted = sincPersisted,
                AverageLatencyMs = sincMetrics.AverageLatencyMs,
                P95LatencyMs = sincMetrics.P95LatencyMs,
                Throughput = sincMetrics.Throughput,
                RecoveryTimeMs = recoveryTimeMs,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário B: Falha de Banco de Dados", "Síncrono", runsSinc));

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (SQL Server 'db' offline por 2 min) - {NumRuns} execuções...");
        var runsAsinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsAsinc = new CancellationTokenSource();
            var stopwatchAsinc = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(AsincUrl, 10, TimeSpan.FromSeconds(300), ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            long stopWorkerTimeMs = stopwatchAsinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "stop", "db");

            await Task.Delay(TimeSpan.FromSeconds(120));
            long startWorkerTimeMs = stopwatchAsinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "start", "db");

            double dbRecoveryMs = 0;
            var recSw = Stopwatch.StartNew();
            int initialDbCount = 0;
            try { initialDbCount = DatabaseHelper.GetEstornosCount(); } catch { }

            while (recSw.Elapsed.TotalSeconds < 30)
            {
                try
                {
                    int current = DatabaseHelper.GetEstornosCount();
                    if (current > initialDbCount)
                    {
                        dbRecoveryMs = recSw.Elapsed.TotalMilliseconds;
                        break;
                    }
                }
                catch
                {
                    // Aguarda banco terminar de subir
                }
                await Task.Delay(100);
            }

            Console.WriteLine("Aguardando banco responder e fila esvaziar (sincronização)...");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            stopwatchAsinc.Stop();

            await WaitForQueueToDrainAsync(asincMetrics.Success);
            int asincPersisted = DatabaseHelper.GetEstornosCount();

            int maxBacklog = asincMetrics.RequestResults
                .Count(r => r.IsSuccess && r.StartTimeMs >= stopWorkerTimeMs && r.StartTimeMs <= startWorkerTimeMs);

            runsAsinc.Add(new ScenarioResult
            {
                Scenario = "Cenário B: Falha de Banco de Dados",
                Flow = "Assíncrono",
                Sent = asincMetrics.Sent,
                Success = asincMetrics.Success,
                Failure = asincMetrics.Failure,
                Persisted = asincPersisted,
                AverageLatencyMs = asincMetrics.AverageLatencyMs,
                P95LatencyMs = asincMetrics.P95LatencyMs,
                Throughput = asincMetrics.Throughput,
                RecoveryTimeMs = dbRecoveryMs,
                MaxQueueDepth = maxBacklog
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário B: Falha de Banco de Dados", "Assíncrono", runsAsinc));
    }

    public static async Task RunScenarioC(string workspacePath, List<ScenarioResult> results)
    {
        Console.WriteLine("\n=== [CENÁRIO C: OPERAÇÃO NORMAL (10 REQ/S - 5 MINUTOS CONTROLE)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (Sem interrupções) - {NumRuns} execuções...");
        var runsSinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsSinc = new CancellationTokenSource();
            var stopwatchSinc = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(SincUrl, 10, TimeSpan.FromSeconds(300), ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            stopwatchSinc.Stop();

            await Task.Delay(3000);
            int sincPersisted = DatabaseHelper.GetEstornosCount();

            runsSinc.Add(new ScenarioResult
            {
                Scenario = "Cenário C: Controle",
                Flow = "Síncrono",
                Sent = sincMetrics.Sent,
                Success = sincMetrics.Success,
                Failure = sincMetrics.Failure,
                Persisted = sincPersisted,
                AverageLatencyMs = sincMetrics.AverageLatencyMs,
                P95LatencyMs = sincMetrics.P95LatencyMs,
                Throughput = sincMetrics.Throughput,
                RecoveryTimeMs = 0,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário C: Controle", "Síncrono", runsSinc));

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (Sem interrupções) - {NumRuns} execuções...");
        var runsAsinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsAsinc = new CancellationTokenSource();
            var stopwatchAsinc = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(AsincUrl, 10, TimeSpan.FromSeconds(300), ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            stopwatchAsinc.Stop();

            await WaitForQueueToDrainAsync(asincMetrics.Success);
            int asincPersisted = DatabaseHelper.GetEstornosCount();

            runsAsinc.Add(new ScenarioResult
            {
                Scenario = "Cenário C: Controle",
                Flow = "Assíncrono",
                Sent = asincMetrics.Sent,
                Success = asincMetrics.Success,
                Failure = asincMetrics.Failure,
                Persisted = asincPersisted,
                AverageLatencyMs = asincMetrics.AverageLatencyMs,
                P95LatencyMs = asincMetrics.P95LatencyMs,
                Throughput = asincMetrics.Throughput,
                RecoveryTimeMs = 0,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário C: Controle", "Assíncrono", runsAsinc));
    }

    public static async Task RunScenarioD(string workspacePath, List<ScenarioResult> results)
    {
        Console.WriteLine("\n=== [CENÁRIO D: TESTE DE CARGA (150 REQ/S - 5 MINUTOS)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (Carga de 150 req/s) - {NumRuns} execuções...");
        var runsSinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsSinc = new CancellationTokenSource();
            var stopwatchSinc = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(SincUrl, 150, TimeSpan.FromSeconds(300), ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            stopwatchSinc.Stop();

            await Task.Delay(3000);
            int sincPersisted = DatabaseHelper.GetEstornosCount();

            runsSinc.Add(new ScenarioResult
            {
                Scenario = "Cenário D: Teste de Carga",
                Flow = "Síncrono",
                Sent = sincMetrics.Sent,
                Success = sincMetrics.Success,
                Failure = sincMetrics.Failure,
                Persisted = sincPersisted,
                AverageLatencyMs = sincMetrics.AverageLatencyMs,
                P95LatencyMs = sincMetrics.P95LatencyMs,
                Throughput = sincMetrics.Throughput,
                RecoveryTimeMs = 0,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário D: Teste de Carga", "Síncrono", runsSinc));

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (Carga de 150 req/s) - {NumRuns} execuções...");
        var runsAsinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsAsinc = new CancellationTokenSource();
            var stopwatchAsinc = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(AsincUrl, 150, TimeSpan.FromSeconds(300), ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(300));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            stopwatchAsinc.Stop();

            var esvaziamentoSw = Stopwatch.StartNew();
            Console.WriteLine("Aguardando fila esvaziar (sincronização no banco)...");
            await WaitForQueueToDrainAsync(asincMetrics.Success);
            esvaziamentoSw.Stop();
            double queueRecoveryTimeMs = esvaziamentoSw.Elapsed.TotalMilliseconds;

            runsAsinc.Add(new ScenarioResult
            {
                Scenario = "Cenário D: Teste de Carga",
                Flow = "Assíncrono",
                Sent = asincMetrics.Sent,
                Success = asincMetrics.Success,
                Failure = asincMetrics.Failure,
                Persisted = DatabaseHelper.GetEstornosCount(),
                AverageLatencyMs = asincMetrics.AverageLatencyMs,
                P95LatencyMs = asincMetrics.P95LatencyMs,
                Throughput = asincMetrics.Throughput,
                RecoveryTimeMs = 0,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário D: Teste de Carga", "Assíncrono", runsAsinc));
    }

    public static async Task RunScenarioE(string workspacePath, List<ScenarioResult> results)
    {
        Console.WriteLine("\n=== [CENÁRIO E: FALHA DE MENSAGERIA (10 REQ/S - 2 MIN OUTAGE RABBITMQ)] ===");

        // --- SÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Síncrono (RabbitMQ 'rabbitmq' offline por 2 min) - {NumRuns} execuções...");
        var runsSinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Síncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsSinc = new CancellationTokenSource();
            var stopwatchSinc = Stopwatch.StartNew();

            var loadTaskSinc = BenchmarkEngine.SendConstantRateLoadAsync(SincUrl, 10, TimeSpan.FromSeconds(300), ctsSinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            DockerManager.RunDockerCommand(workspacePath, "stop", "rabbitmq");

            await Task.Delay(TimeSpan.FromSeconds(120));
            long startServiceTimeMs = stopwatchSinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "start", "rabbitmq");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsSinc.Cancel();

            var sincMetrics = await loadTaskSinc;
            stopwatchSinc.Stop();

            await Task.Delay(3000);
            int sincPersisted = DatabaseHelper.GetEstornosCount();

            runsSinc.Add(new ScenarioResult
            {
                Scenario = "Cenário E: Falha de Mensageria",
                Flow = "Síncrono",
                Sent = sincMetrics.Sent,
                Success = sincMetrics.Success,
                Failure = sincMetrics.Failure,
                Persisted = sincPersisted,
                AverageLatencyMs = sincMetrics.AverageLatencyMs,
                P95LatencyMs = sincMetrics.P95LatencyMs,
                Throughput = sincMetrics.Throughput,
                RecoveryTimeMs = 0,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário E: Falha de Mensageria", "Síncrono", runsSinc));

        // --- ASSÍNCRONO ---
        Console.WriteLine($"\nIniciando Teste Assíncrono (RabbitMQ 'rabbitmq' offline por 2 min) - {NumRuns} execuções...");
        var runsAsinc = new List<ScenarioResult>();

        for (int run = 1; run <= NumRuns; run++)
        {
            Console.WriteLine($"\n[Assíncrono] Execução {run}/{NumRuns}...");
            DockerManager.EnsureAllServicesRunning(workspacePath);
            DatabaseHelper.ClearEstornos();

            using var ctsAsinc = new CancellationTokenSource();
            var stopwatchAsinc = Stopwatch.StartNew();

            var loadTaskAsinc = BenchmarkEngine.SendConstantRateLoadAsync(AsincUrl, 10, TimeSpan.FromSeconds(300), ctsAsinc.Token);

            await Task.Delay(TimeSpan.FromSeconds(60));
            long stopWorkerTimeMs = stopwatchAsinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "stop", "rabbitmq");

            await Task.Delay(TimeSpan.FromSeconds(120));
            long startWorkerTimeMs = stopwatchAsinc.ElapsedMilliseconds;
            DockerManager.RunDockerCommand(workspacePath, "start", "rabbitmq");

            var esvaziamentoSw = Stopwatch.StartNew();
            Console.WriteLine("Aguardando RabbitMQ estabilizar e mensagens serem processadas...");

            await Task.Delay(TimeSpan.FromSeconds(120));
            ctsAsinc.Cancel();

            var asincMetrics = await loadTaskAsinc;
            stopwatchAsinc.Stop();

            double recoveryTimeMs = 0;
            var firstSuccess = asincMetrics.RequestResults
                .Where(r => r.IsSuccess && r.StartTimeMs >= startWorkerTimeMs)
                .OrderBy(r => r.StartTimeMs)
                .Cast<RequestResult?>()
                .FirstOrDefault();

            if (firstSuccess.HasValue)
            {
                recoveryTimeMs = (firstSuccess.Value.StartTimeMs + firstSuccess.Value.LatencyMs) - startWorkerTimeMs;
            }

            await WaitForQueueToDrainAsync(asincMetrics.Success);
            esvaziamentoSw.Stop();

            runsAsinc.Add(new ScenarioResult
            {
                Scenario = "Cenário E: Falha de Mensageria",
                Flow = "Assíncrono",
                Sent = asincMetrics.Sent,
                Success = asincMetrics.Success,
                Failure = asincMetrics.Failure,
                Persisted = DatabaseHelper.GetEstornosCount(),
                AverageLatencyMs = asincMetrics.AverageLatencyMs,
                P95LatencyMs = asincMetrics.P95LatencyMs,
                Throughput = asincMetrics.Throughput,
                RecoveryTimeMs = recoveryTimeMs,
                MaxQueueDepth = 0
            });
        }
        results.Add(ReportGenerator.ConsolidateResults("Cenário E: Falha de Mensageria", "Assíncrono", runsAsinc));
    }

    private static async Task WaitForQueueToDrainAsync(int targetCount)
    {
        var sw = Stopwatch.StartNew();
        int lastCount = -1;
        int unchangedIterations = 0;

        while (sw.Elapsed.TotalSeconds < 120)
        {
            try
            {
                int currentCount = DatabaseHelper.GetEstornosCount();
                if (currentCount >= targetCount) break;

                if (currentCount == lastCount && currentCount > 0)
                {
                    unchangedIterations++;
                    if (unchangedIterations >= 10) break; // Fila estabilizou após 5s sem novas inserções
                }
                else
                {
                    unchangedIterations = 0;
                    lastCount = currentCount;
                }
            }
            catch
            {
                // Ignora exceções temporárias de reconexão
            }

            await Task.Delay(500);
        }
    }
}

