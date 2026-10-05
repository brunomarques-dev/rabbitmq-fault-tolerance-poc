using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Estornos.TestRunner;

public class DiagnosticApiSample
{
    public DateTime Timestamp { get; set; }
    public double ElapsedSeconds { get; set; }
    public int Consumers { get; set; }
    public int MessagesReady { get; set; }
    public int MessagesUnacknowledged { get; set; }
    public int MessagesTotal { get; set; }
    public int ConnectionCount { get; set; }
    public int ChannelCount { get; set; }
}

public class DiagnosticRunResult
{
    public int RunNumber { get; set; }
    public int Sent { get; set; }
    public int HTTPAccepted { get; set; }
    public int InitialPersisted { get; set; }
    public int FinalPersisted { get; set; }
    public bool DrainCompleted { get; set; }
    public double? BacklogDrainTimeMs { get; set; }
    public string Classification { get; set; } = "ESTADO INDETERMINADO";
    
    // Timeline Checkpoints
    public string T0_State { get; set; } = "";
    public string T60_PreStop_State { get; set; } = "";
    public DateTime OutageStartTime { get; set; }
    public DateTime OutageEndTime { get; set; }
    public string FirstEventAfterOutage { get; set; } = "";
    public string OutageState { get; set; } = "";
    public string FirstConnectionRecoveredSignal { get; set; } = "";
    public string FirstChannelRecoveredSignal { get; set; } = "";
    public string FirstConsumerRegisteredSignal { get; set; } = "";
    public string FirstMessageDeliveredSignal { get; set; } = "";
    public string FirstAckSignal { get; set; } = "";
    public string BacklogDrainStartSignal { get; set; } = "";
    public string BacklogZeroSignal { get; set; } = "";
    
    public List<DiagnosticApiSample> MgmtSamples { get; set; } = new();
    public List<string> WorkerLogs { get; set; } = new();
}

public static class DiagnosticRunner
{
    private static readonly HttpClient HttpMgmt;
    private const string AsincUrl = "http://localhost:5000/api/estornos/asinc";

    static DiagnosticRunner()
    {
        HttpMgmt = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        var bytes = Encoding.ASCII.GetBytes("guest:guest");
        HttpMgmt.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(bytes));
    }

    public static async Task RunDiagnosticCampaignAsync(string workspacePath, int numRuns = 5)
    {
        string timestampStr = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");
        string diagOutputDir = Path.Combine(workspacePath, "results", "diagnostic-scenario-e", timestampStr);
        Directory.CreateDirectory(diagOutputDir);

        Console.ForegroundColor = ConsoleColor.Magenta;
        Console.WriteLine("\n========================================================================");
        Console.WriteLine($"   INICIANDO TESTE DIAGNÓSTICO DO CENÁRIO E ({numRuns} REPETIÇÕES)");
        Console.WriteLine($"   Diretório de Saída: {diagOutputDir}");
        Console.WriteLine("========================================================================");
        Console.ResetColor();

        // 0. Rebuild worker container with diagnostic instrumentation
        Console.WriteLine("[DIAGNOSTIC] Reconstruindo container 'estornos-consumer-worker' com instrumentação...");
        DockerManager.RunDockerCommand(workspacePath, "up -d --build", "estornos-consumer-worker");
        await Task.Delay(5000);

        var runResults = new List<DiagnosticRunResult>();

        for (int run = 1; run <= numRuns; run++)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"\n------------------------------------------------------------------------");
            Console.WriteLine($" [TESTE DIAGNÓSTICO] Execução {run}/{numRuns} (Cenário E Assíncrono)...");
            Console.WriteLine($"------------------------------------------------------------------------");
            Console.ResetColor();

            // 1. Preparação e Limpeza
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);
            await DockerManager.RunWarmUpAsync(workspacePath);
            await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);

            // Confirmação pré-teste
            var preDb = DatabaseHelper.GetDbMetrics();
            var preMgmt = await FetchMgmtStatusAsync();
            Console.WriteLine($"[CONFIRMAÇÃO PRÉ-TESTE RUN {run}] DB={preDb.TotalPersisted} | QueueMsgs={preMgmt.MessagesTotal} | Consumers={preMgmt.Consumers} | Connections={preMgmt.ConnectionCount} | Channels={preMgmt.ChannelCount}");
            
            if (preDb.TotalPersisted != 0 || preMgmt.MessagesTotal != 0)
            {
                throw new Exception($"[ERRO DIAGNÓSTICO] Ambiente não está zerado pré-run {run}! DB: {preDb.TotalPersisted}, Queue: {preMgmt.MessagesTotal}");
            }

            var runResult = new DiagnosticRunResult
            {
                RunNumber = run,
                T0_State = $"Consumers={preMgmt.Consumers}, Ready={preMgmt.MessagesReady}, Unacked={preMgmt.MessagesUnacknowledged}, TotalMsgs={preMgmt.MessagesTotal}, Conns={preMgmt.ConnectionCount}, Chans={preMgmt.ChannelCount}"
            };

            using var cts = new CancellationTokenSource();
            var mgmtSamples = new List<DiagnosticApiSample>();

            // Monitoring Task (Management API every 1s)
            var monitorTask = Task.Run(async () =>
            {
                var monitorSw = Stopwatch.StartNew();
                while (!cts.Token.IsCancellationRequested)
                {
                    var s = await FetchMgmtStatusAsync();
                    s.ElapsedSeconds = monitorSw.Elapsed.TotalSeconds;
                    mgmtSamples.Add(s);

                    try
                    {
                        await Task.Delay(1000, cts.Token);
                    }
                    catch (TaskCanceledException) { break; }
                }
            });

            var runSw = Stopwatch.StartNew();

            // Iniciar Carga de 5 req/s por 300s
            var loadTask = BenchmarkEngine.SendConstantRateLoadAsync(
                AsincUrl, 5, TimeSpan.FromSeconds(300), "Cenário E Diagnóstico", "Assíncrono", run, cts.Token);

            // Aguardar 60s
            await Task.Delay(TimeSpan.FromSeconds(60));

            // Checkpoint t≈60s antes do stop
            var t60Mgmt = await FetchMgmtStatusAsync();
            runResult.T60_PreStop_State = $"Consumers={t60Mgmt.Consumers}, Ready={t60Mgmt.MessagesReady}, Unacked={t60Mgmt.MessagesUnacknowledged}, TotalMsgs={t60Mgmt.MessagesTotal}, Conns={t60Mgmt.ConnectionCount}, Chans={t60Mgmt.ChannelCount}";
            Console.WriteLine($"[CHECKPOINT t≈60s PRE-STOP RUN {run}] {runResult.T60_PreStop_State}");

            // Parar RabbitMQ
            runResult.OutageStartTime = DateTime.UtcNow;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[OUTAGE START RUN {run}] Desligando RabbitMQ ({runResult.OutageStartTime:yyyy-MM-dd HH:mm:ss.fff})...");
            Console.ResetColor();
            DockerManager.RunDockerCommand(workspacePath, "stop", "rabbitmq");

            // Aguardar 120s de outage (t=60s até t=180s)
            await Task.Delay(TimeSpan.FromSeconds(120));

            // Religar RabbitMQ
            runResult.OutageEndTime = DateTime.UtcNow;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[OUTAGE END RUN {run}] Religando RabbitMQ ({runResult.OutageEndTime:yyyy-MM-dd HH:mm:ss.fff})...");
            Console.ResetColor();
            DockerManager.RunDockerCommand(workspacePath, "start", "rabbitmq");

            // Aguardar restante do envio (120s até completar 300s)
            await Task.Delay(TimeSpan.FromSeconds(120));
            cts.Cancel();

            var loadMetrics = await loadTask;
            runResult.Sent = loadMetrics.Sent;
            runResult.HTTPAccepted = loadMetrics.HTTPAccepted;

            try { await monitorTask; } catch { }

            // Fase de Drenagem (timeout = 600s)
            Console.WriteLine($"[FASE DE DRENAGEM RUN {run}] Monitorando por até 600s...");
            var drainSw = Stopwatch.StartNew();
            bool drainCompleted = false;
            int consecutiveZeros = 0;

            while (drainSw.Elapsed.TotalSeconds < 600)
            {
                var s = await FetchMgmtStatusAsync();
                s.ElapsedSeconds = runSw.Elapsed.TotalSeconds;
                mgmtSamples.Add(s);

                int dbCount = DatabaseHelper.GetEstornosCount();
                if (dbCount >= loadMetrics.HTTPAccepted && s.MessagesTotal == 0)
                {
                    consecutiveZeros++;
                    if (consecutiveZeros >= 2)
                    {
                        drainCompleted = true;
                        break;
                    }
                }
                else
                {
                    consecutiveZeros = 0;
                }

                await Task.Delay(1000);
            }

            drainSw.Stop();
            runSw.Stop();

            var finalDb = DatabaseHelper.GetDbMetrics();
            runResult.InitialPersisted = DatabaseHelper.GetEstornosCount();
            runResult.FinalPersisted = finalDb.UniquePersisted;
            runResult.DrainCompleted = drainCompleted;
            runResult.BacklogDrainTimeMs = drainCompleted ? drainSw.Elapsed.TotalMilliseconds : null;
            runResult.MgmtSamples = mgmtSamples;

            // Extrair Logs do Worker para esta Run
            runResult.WorkerLogs = FetchWorkerLogsSince(workspacePath, runResult.OutageStartTime.AddSeconds(-70));

            // Analisar a Run com base nos logs e amostragem
            AnalyzeRunCheckpointsAndClassification(runResult);

            // Salvar artefatos individuais da Run
            SaveRunArtifacts(diagOutputDir, runResult);

            runResults.Add(runResult);
        }

        // Salvar Relatório Consolidado do Diagnóstico
        SaveDiagnosticReport(workspacePath, diagOutputDir, runResults);
    }

    private static async Task<DiagnosticApiSample> FetchMgmtStatusAsync()
    {
        try
        {
            var resQueue = await HttpMgmt.GetAsync("http://localhost:15672/api/queues/%2F/estornos-queue");
            int consumers = 0, ready = 0, unacked = 0, total = 0;

            if (resQueue.IsSuccessStatusCode)
            {
                var json = await resQueue.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                consumers = root.TryGetProperty("consumers", out var cElem) ? cElem.GetInt32() : 0;
                ready = root.TryGetProperty("messages_ready", out var rElem) ? rElem.GetInt32() : 0;
                unacked = root.TryGetProperty("messages_unacknowledged", out var uElem) ? uElem.GetInt32() : 0;
                total = root.TryGetProperty("messages", out var tElem) ? tElem.GetInt32() : 0;
            }

            var resConn = await HttpMgmt.GetAsync("http://localhost:15672/api/connections");
            int connCount = 0;
            if (resConn.IsSuccessStatusCode)
            {
                var jsonConn = await resConn.Content.ReadAsStringAsync();
                using var docConn = JsonDocument.Parse(jsonConn);
                connCount = docConn.RootElement.GetArrayLength();
            }

            var resChan = await HttpMgmt.GetAsync("http://localhost:15672/api/channels");
            int chanCount = 0;
            if (resChan.IsSuccessStatusCode)
            {
                var jsonChan = await resChan.Content.ReadAsStringAsync();
                using var docChan = JsonDocument.Parse(jsonChan);
                chanCount = docChan.RootElement.GetArrayLength();
            }

            return new DiagnosticApiSample
            {
                Timestamp = DateTime.UtcNow,
                Consumers = consumers,
                MessagesReady = ready,
                MessagesUnacknowledged = unacked,
                MessagesTotal = total,
                ConnectionCount = connCount,
                ChannelCount = chanCount
            };
        }
        catch
        {
            return new DiagnosticApiSample
            {
                Timestamp = DateTime.UtcNow,
                Consumers = 0,
                MessagesReady = 0,
                MessagesUnacknowledged = 0,
                MessagesTotal = 0,
                ConnectionCount = 0,
                ChannelCount = 0
            };
        }
    }

    private static List<string> FetchWorkerLogsSince(string workspacePath, DateTime sinceTime)
    {
        var logs = new List<string>();
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "compose logs estornos-consumer-worker",
                WorkingDirectory = workspacePath,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = Process.Start(psi);
            if (process != null)
            {
                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();
                process.WaitForExit();

                var lines = (output + "\n" + error).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                logs.AddRange(lines);
            }
        }
        catch { }

        if (logs.Count == 0 || logs.Any(l => l.Contains("No such container")))
        {
            try
            {
                var psi2 = new ProcessStartInfo
                {
                    FileName = "docker",
                    Arguments = "logs apisinc-estornos-consumer-worker-1",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using var process2 = Process.Start(psi2);
                if (process2 != null)
                {
                    string output = process2.StandardOutput.ReadToEnd();
                    string error = process2.StandardError.ReadToEnd();
                    process2.WaitForExit();

                    var lines = (output + "\n" + error).Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    logs.Clear();
                    logs.AddRange(lines);
                }
            }
            catch { }
        }

        return logs;
    }

    private static void AnalyzeRunCheckpointsAndClassification(DiagnosticRunResult result)
    {
        // 1. Procurar eventos nos logs do Worker após o término do Outage
        var postOutageLogs = result.WorkerLogs
            .Where(l => l.Contains("[DIAGNOSTIC_EVENT]") || l.Contains("[DIAGNOSTIC_STATE]"))
            .ToList();

        var recoverySuccessLog = postOutageLogs.FirstOrDefault(l => l.Contains("Event=RecoverySucceeded"));
        var connErrorLog = postOutageLogs.FirstOrDefault(l => l.Contains("Event=ConnectionRecoveryError"));
        var connShutdownLog = postOutageLogs.FirstOrDefault(l => l.Contains("Event=ConnectionShutdown"));
        var modelShutdownLog = postOutageLogs.FirstOrDefault(l => l.Contains("Event=ModelShutdown"));
        var consumerRegisteredLog = postOutageLogs.FirstOrDefault(l => l.Contains("Event=ConsumerRegistered"));
        var firstAckLog = postOutageLogs.FirstOrDefault(l => l.Contains("Event=MessageAcked"));

        result.FirstEventAfterOutage = postOutageLogs.FirstOrDefault(l => l.Contains("Event=")) ?? "Nenhum evento registrado pelo worker";
        result.FirstConnectionRecoveredSignal = recoverySuccessLog ?? (connErrorLog ?? "Sem sinal de recuperação de conexão");
        result.FirstChannelRecoveredSignal = modelShutdownLog != null ? $"ModelShutdown: {modelShutdownLog}" : "Canal mantido ou reaberto";
        result.FirstConsumerRegisteredSignal = consumerRegisteredLog ?? "Sem registro de re-inscrição do consumidor";
        result.FirstAckSignal = firstAckLog ?? "Nenhum ACK após restauração";

        // Métrica da Management API pós-restauração
        var postOutageMgmt = result.MgmtSamples
            .Where(s => s.Timestamp >= result.OutageEndTime)
            .ToList();

        bool hasConsumerInMgmt = postOutageMgmt.Any(s => s.Consumers > 0);
        bool hasUnackedInMgmt = postOutageMgmt.Any(s => s.MessagesUnacknowledged > 0);
        bool hasConnInMgmt = postOutageMgmt.Any(s => s.ConnectionCount > 0);

        // Classificação
        if (result.DrainCompleted && result.FinalPersisted >= result.HTTPAccepted)
        {
            result.Classification = "RECUPERAÇÃO COMPLETA";
        }
        else if (hasConnInMgmt && !hasConsumerInMgmt)
        {
            result.Classification = "CONEXÃO RECUPEROU MAS CONSUMER NÃO";
        }
        else if (!hasConnInMgmt)
        {
            result.Classification = "CONEXÃO NÃO RECUPEROU";
        }
        else if (hasConsumerInMgmt && !result.DrainCompleted)
        {
            result.Classification = "CHANNEL NÃO RECUPEROU";
        }
        else
        {
            result.Classification = "ESTADO INDETERMINADO";
        }
    }

    private static void SaveRunArtifacts(string diagOutputDir, DiagnosticRunResult result)
    {
        string runDir = Path.Combine(diagOutputDir, $"run_{result.RunNumber}");
        Directory.CreateDirectory(runDir);

        // Management API CSV
        string csvFile = Path.Combine(runDir, "queue_mgmt_1s.csv");
        using (var writer = new StreamWriter(csvFile, false, Encoding.UTF8))
        {
            writer.WriteLine("Timestamp,ElapsedSeconds,Consumers,MessagesReady,MessagesUnacknowledged,MessagesTotal,ConnectionCount,ChannelCount");
            foreach (var s in result.MgmtSamples)
            {
                writer.WriteLine($"{s.Timestamp:yyyy-MM-dd HH:mm:ss.fff},{s.ElapsedSeconds:F2},{s.Consumers},{s.MessagesReady},{s.MessagesUnacknowledged},{s.MessagesTotal},{s.ConnectionCount},{s.ChannelCount}");
            }
        }

        // Worker Logs
        string logFile = Path.Combine(runDir, "worker_diagnostic.log");
        File.WriteAllLines(logFile, result.WorkerLogs, Encoding.UTF8);

        Console.WriteLine($"[ARTEFATOS SALVOS RUN {result.RunNumber}] {runDir}");
    }

    private static void SaveDiagnosticReport(string workspacePath, string diagOutputDir, List<DiagnosticRunResult> results)
    {
        string summaryMdFile = Path.Combine(diagOutputDir, "diagnostic_summary.md");
        var sb = new StringBuilder();

        sb.AppendLine("# Relatório Diagnóstico — Cenário E (Falha de Mensageria)");
        sb.AppendLine($"Gerado em: {DateTime.Now:dd/MM/yyyy HH:mm:ss}");
        sb.AppendLine();
        sb.AppendLine("## 1. Tabela Comparativa entre as 5 Execuções Diagnósticas");
        sb.AppendLine();
        sb.AppendLine("| Run | RecoverySucceeded | Connection IsOpen pós-retorno | Channel IsOpen | Consumers na fila | Primeiro consumo pós-retorno | Drenou fila? | Resultado |");
        sb.AppendLine("|:---:|:-----------------:|:-----------------------------:|:--------------:|:-----------------:|:----------------------------:|:------------:|:---------:|");

        foreach (var r in results)
        {
            var postOutageMgmt = r.MgmtSamples.Where(s => s.Timestamp >= r.OutageEndTime).ToList();
            bool hasConn = postOutageMgmt.Any(s => s.ConnectionCount > 0);
            int consumers = postOutageMgmt.Count > 0 ? postOutageMgmt.Max(s => s.Consumers) : 0;
            bool recSucc = r.FirstConnectionRecoveredSignal.Contains("RecoverySucceeded");
            string firstCons = r.FirstAckSignal.Contains("MessageAcked") ? "SIM (ACK)" : "NÃO (0)";
            string drainStr = r.DrainCompleted ? "SIM" : "NÃO";

            sb.AppendLine($"| {r.RunNumber} | {(recSucc ? "SIM" : "NÃO / Não Detectado")} | {(hasConn ? "SIM" : "NÃO")} | {(consumers > 0 ? "SIM" : "NÃO")} | {consumers} | {firstCons} | {drainStr} | **{r.Classification}** |");
        }

        sb.AppendLine();
        sb.AppendLine("## 2. Detalhamento por Execução");
        foreach (var r in results)
        {
            sb.AppendLine($"### Execução {r.RunNumber} — Classificação: {r.Classification}");
            sb.AppendLine($"- **Aceitas HTTP**: {r.HTTPAccepted} | **Persistidas Finais**: {r.FinalPersisted}");
            sb.AppendLine($"- **t=0 State**: `{r.T0_State}`");
            sb.AppendLine($"- **t≈60s Pre-Stop State**: `{r.T60_PreStop_State}`");
            sb.AppendLine($"- **Outage Period**: `{r.OutageStartTime:HH:mm:ss.fff}` a `{r.OutageEndTime:HH:mm:ss.fff}`");
            sb.AppendLine($"- **Primeiro Evento Pós-Outage**: `{r.FirstEventAfterOutage}`");
            sb.AppendLine($"- **Sinal de Conexão Recuperada**: `{r.FirstConnectionRecoveredSignal}`");
            sb.AppendLine($"- **Sinal de Consumer Registrado**: `{r.FirstConsumerRegisteredSignal}`");
            sb.AppendLine($"- **Sinal de Primeiro ACK**: `{r.FirstAckSignal}`");
            sb.AppendLine();
        }

        File.WriteAllText(summaryMdFile, sb.ToString(), Encoding.UTF8);
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"\n========================================================================");
        Console.WriteLine($"   RELATÓRIO DIAGNÓSTICO CONCLUÍDO!");
        Console.WriteLine($"   Arquivo: {summaryMdFile}");
        Console.WriteLine("========================================================================\n");
        Console.ResetColor();
    }
}
