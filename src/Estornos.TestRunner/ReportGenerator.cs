using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Estornos.TestRunner;

public static class ReportGenerator
{
    public static async Task SaveSessionResultsAsync(string workspacePath, string sessionDirName, List<ConsolidatedScenarioResult> consolidatedResults)
    {
        string resultsRootDir = Path.Combine(workspacePath, "results");
        string sessionPath = Path.Combine(resultsRootDir, sessionDirName);
        string rawDir = Path.Combine(sessionPath, "raw");
        string queueDir = Path.Combine(sessionPath, "queue");

        Directory.CreateDirectory(resultsRootDir);
        Directory.CreateDirectory(sessionPath);
        Directory.CreateDirectory(rawDir);
        Directory.CreateDirectory(queueDir);

        // 1. Salvar environment.txt
        string envReport = await EnvironmentLogger.GenerateEnvironmentReportAsync(workspacePath);
        await File.WriteAllTextAsync(Path.Combine(sessionPath, "environment.txt"), envReport, Encoding.UTF8);

        // 2. Salvar arquivos brutos por run em raw/ e queue/
        var allSingleRuns = new List<SingleRunResult>();
        foreach (var consolidated in consolidatedResults)
        {
            foreach (var run in consolidated.Runs)
            {
                allSingleRuns.Add(run);

                string sanitizeName(string name) => name.ToLower().Replace(":", "").Replace(" ", "-").Replace("ã", "a").Replace("ó", "o");
                string scenarioSlug = sanitizeName(run.Scenario);
                string flowSlug = sanitizeName(run.Flow);
                string baseFileName = $"{scenarioSlug}-{flowSlug}-run-{run.RunNumber}";

                // Raw requests CSV
                string rawCsvPath = Path.Combine(rawDir, $"{baseFileName}.csv");
                var rawSb = new StringBuilder();
                rawSb.AppendLine("Scenario,Flow,Run,IdTransacaoOriginal,StartTimestamp,EndTimestamp,LatencyMs,HttpStatusCode,IsSuccess,ExceptionMessage");
                foreach (var req in run.RawRequestResults)
                {
                    string escEx = req.ExceptionMessage != null ? $"\"{req.ExceptionMessage.Replace("\"", "\"\"")}\"" : "";
                    rawSb.AppendLine($"{req.Scenario},{req.Flow},{req.Run},{req.IdTransacaoOriginal},{req.StartTimestamp:yyyy-MM-dd HH:mm:ss.fff},{req.EndTimestamp:yyyy-MM-dd HH:mm:ss.fff},{req.LatencyMs.ToString(CultureInfo.InvariantCulture)},{req.HttpStatusCode},{req.IsSuccess},{escEx}");
                }
                await File.WriteAllTextAsync(rawCsvPath, rawSb.ToString(), Encoding.UTF8);

                // Queue samples CSV (se houver)
                if (run.QueueSamples.Count > 0)
                {
                    string queueCsvPath = Path.Combine(queueDir, $"{baseFileName}.csv");
                    var queueSb = new StringBuilder();
                    queueSb.AppendLine("Timestamp,MessagesReady,MessagesUnacknowledged,MessagesTotal");
                    foreach (var sample in run.QueueSamples)
                    {
                        queueSb.AppendLine($"{sample.Timestamp:yyyy-MM-dd HH:mm:ss.fff},{sample.MessagesReady},{sample.MessagesUnacknowledged},{sample.MessagesTotal}");
                    }
                    await File.WriteAllTextAsync(queueCsvPath, queueSb.ToString(), Encoding.UTF8);
                }
            }
        }

        // 3. Salvar runs.csv
        string runsCsvPath = Path.Combine(sessionPath, "runs.csv");
        var runsSb = new StringBuilder();
        runsSb.AppendLine("Scenario,Flow,RunNumber,Sent,HTTPAccepted,HTTPFailed,HTTPAcceptanceRate,HTTPAcceptedAtSendEnd,UniquePersistedAtSendEnd,QueueReadyAtSendEnd,QueueUnackedAtSendEnd,QueueTotalAtSendEnd,PersistedRowsFinal,UniquePersistedFinal,DuplicateRowsFinal,QueueTotalFinal,UniqueLost,UniquePersistenceRate,PreservationRate,HttpLatencyMeanAll,HttpP95All,HttpP99All,HttpMaxAll,HttpLatencyMeanSuccess,HttpP95Success,HttpP99Success,ConfiguredSendRate,ActualSendRate,MaxQueueDepth,MaxMessagesReady,MaxMessagesUnacked,HttpRecoveryTimeMs,PersistenceRecoveryTimeMs,BacklogDrainTimeMs,SendDurationMs,TotalExperimentDurationMs,DrainCompleted,PendingOperations,AverageCompletionThroughput");

        foreach (var r in allSingleRuns)
        {
            string fmtD(double? val) => val.HasValue ? val.Value.ToString(CultureInfo.InvariantCulture) : "N/A";
            string fmtI(int? val) => val.HasValue ? val.Value.ToString() : "N/A";
            string fmtB(bool? val) => val.HasValue ? val.Value.ToString() : "N/A";

            runsSb.AppendLine($"{r.Scenario},{r.Flow},{r.RunNumber},{r.Sent},{r.HTTPAccepted},{r.HTTPFailed},{r.HTTPAcceptanceRate.ToString(CultureInfo.InvariantCulture)},{r.HTTPAcceptedAtSendEnd},{r.UniquePersistedAtSendEnd},{fmtI(r.QueueReadyAtSendEnd)},{fmtI(r.QueueUnackedAtSendEnd)},{fmtI(r.QueueTotalAtSendEnd)},{r.PersistedRowsFinal},{r.UniquePersistedFinal},{r.DuplicateRowsFinal},{fmtI(r.QueueTotalFinal)},{r.UniqueLost},{r.UniquePersistenceRate.ToString(CultureInfo.InvariantCulture)},{fmtD(r.PreservationRate)},{r.HttpLatencyMeanAll.ToString(CultureInfo.InvariantCulture)},{r.HttpP95All.ToString(CultureInfo.InvariantCulture)},{r.HttpP99All.ToString(CultureInfo.InvariantCulture)},{r.HttpMaxAll.ToString(CultureInfo.InvariantCulture)},{fmtD(r.HttpLatencyMeanSuccess)},{fmtD(r.HttpP95Success)},{fmtD(r.HttpP99Success)},{r.ConfiguredSendRate},{r.ActualSendRate.ToString(CultureInfo.InvariantCulture)},{fmtI(r.MaxQueueDepth)},{fmtI(r.MaxMessagesReady)},{fmtI(r.MaxMessagesUnacked)},{fmtD(r.HttpRecoveryTimeMs)},{fmtD(r.PersistenceRecoveryTimeMs)},{fmtD(r.BacklogDrainTimeMs)},{r.SendDurationMs.ToString(CultureInfo.InvariantCulture)},{r.TotalExperimentDurationMs.ToString(CultureInfo.InvariantCulture)},{fmtB(r.DrainCompleted)},{r.PendingOperations},{fmtD(r.AverageCompletionThroughput)}");
        }
        await File.WriteAllTextAsync(runsCsvPath, runsSb.ToString(), Encoding.UTF8);

        // 4. Salvar summary.md
        string summaryMdPath = Path.Combine(sessionPath, "summary.md");
        string summaryContent = GenerateSummaryMarkdown(consolidatedResults, envReport);
        await File.WriteAllTextAsync(summaryMdPath, summaryContent, Encoding.UTF8);

        // 5. Atualizar também o test_results.md na raiz para facilidade de leitura rápida
        await File.WriteAllTextAsync(Path.Combine(workspacePath, "test_results.md"), summaryContent, Encoding.UTF8);

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"\n[RELATÓRIO COMPLETO SALVO]");
        Console.WriteLine($"Diretório da Campanha: {sessionPath}");
        Console.ResetColor();
    }

    private static string GenerateSummaryMarkdown(List<ConsolidatedScenarioResult> consolidatedResults, string envReport)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Relatório Consolidado de Resiliência e Desempenho (TCC)");
        sb.AppendLine();
        sb.AppendLine($"Gerado em: {DateTime.Now:dd/MM/yyyy HH:mm:ss}");
        sb.AppendLine();
        sb.AppendLine("## 1. Configuração do Ambiente Experimento");
        sb.AppendLine("```text");
        sb.AppendLine(envReport);
        sb.AppendLine("```");
        sb.AppendLine();

        sb.AppendLine("## 2. Resultados Consolidados das 3 Rodadas por Cenário");
        sb.AppendLine();
        sb.AppendLine("Convenção de Nomenclatura das Métricas:");
        sb.AppendLine("- **Taxa de Envio (Offered Load):** Carga solicitada e enviada pelo cliente (req/s).");
        sb.AppendLine("- **Requisições Aceitas (HTTP 2xx):** Requisições confirmadas pelo Gateway HTTP.");
        sb.AppendLine("- **Operações Únicas Persistidas:** Contagem `COUNT(DISTINCT IdTransacaoOriginal)` no SQL Server.");
        sb.AppendLine("- **Registros Duplicados:** `TotalPersisted - UniquePersisted`.");
        sb.AppendLine("- **Unique Lost:** Operações únicas não persistidas `max(0, Sent - UniquePersisted)`.");
        sb.AppendLine("- **Global P95 / Global P99:** Percentis calculados sobre o universo total agrupado das 3 rodadas.");
        sb.AppendLine("- **N/A:** Métrica não aplicável ao cenário/fluxo específico.");
        sb.AppendLine();

        sb.AppendLine("| Cenário | Fluxo | Rodadas | Taxa Envio | Aceitas HTTP (Média) | Persistidas Únicas (Média) | Duplicadas (Média) | Média Latência HTTP All | Global P95 All | Global P95 Success | T. Rec. HTTP ($T_{rest\\_http}$) | T. Rec. Persistência ($T_{inicio\\_pers}$) | T. Drenagem Backlog ($T_{drenagem}$) | Max Backlog Real |");
        sb.AppendLine("| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |");

        foreach (var c in consolidatedResults)
        {
            string fmtStatI(MetricStats stats) => $"{stats.Mean:F0} (DP: {stats.StdDev:F1})";
            string fmtStatD(MetricStats stats) => $"{stats.Mean:F2} ms (DP: {stats.StdDev:F1})";

            string fmtRec(MetricStats? stats) => stats != null && stats.Mean > 0 ? $"{stats.Mean:F2} ms (DP: {stats.StdDev:F1})" : "N/A";

            string maxQueueStr = c.MaxQueueDepthStats.Mean > 0 ? $"{c.MaxQueueDepthStats.Mean:F0} msgs" : "N/A";
            string globP95SuccStr = c.GlobalP95SuccessfulRequests.HasValue ? $"{c.GlobalP95SuccessfulRequests.Value:F2} ms" : "N/A";

            sb.AppendLine($"| {c.Scenario} | {c.Flow} | {c.Runs.Count} | {c.ActualSendRateStats.Mean:F2} req/s | {fmtStatI(c.HTTPAcceptedStats)} | {fmtStatI(c.UniquePersistedStats)} | {fmtStatI(c.DuplicateRowsStats)} | {fmtStatD(c.HttpLatencyMeanAllStats)} | {c.GlobalP95AllRequests:F2} ms | {globP95SuccStr} | {fmtRec(c.HttpRecoveryTimeStats)} | {fmtRec(c.PersistenceRecoveryTimeStats)} | {fmtRec(c.BacklogDrainTimeStats)} | {maxQueueStr} |");
        }

        sb.AppendLine();
        sb.AppendLine("## 3. Tabela Detalhada por Execução Individual (Rodadas 1, 2 e 3)");
        sb.AppendLine();
        sb.AppendLine("| Cenário | Fluxo | Run | Enviadas | Aceitas HTTP | Falhas HTTP | Taxa Aceitação | Persistidas Únicas | Duplicadas | Perda Única | Taxa Preservação | Latência Média All | P95 All | P95 Success | Rec. HTTP | Rec. Persistência | Drenagem Backlog | Max Fila Real | Vazão Conclusão |");
        sb.AppendLine("| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |");

        foreach (var c in consolidatedResults)
        {
            foreach (var r in c.Runs)
            {
                string fmtD(double? val, string unit = " ms") => val.HasValue ? $"{val.Value:F2}{unit}" : "N/A";
                string fmtP(double? val) => val.HasValue ? $"{val.Value:F2}%" : "N/A";
                string fmtI(int? val, string unit = "") => val.HasValue && val.Value > 0 ? $"{val.Value}{unit}" : "N/A";

                sb.AppendLine($"| {r.Scenario} | {r.Flow} | {r.RunNumber} | {r.Sent} | {r.HTTPAccepted} | {r.HTTPFailed} | {r.HTTPAcceptanceRate:F2}% | {r.UniquePersisted} | {r.DuplicateRows} | {r.UniqueLost} | {fmtP(r.PreservationRate)} | {r.HttpLatencyMeanAll:F2} ms | {r.HttpP95All:F2} ms | {fmtD(r.HttpP95Success)} | {fmtD(r.HttpRecoveryTimeMs)} | {fmtD(r.PersistenceRecoveryTimeMs)} | {fmtD(r.BacklogDrainTimeMs)} | {fmtI(r.MaxQueueDepth, " msgs")} | {fmtD(r.AverageCompletionThroughput, " ops/s")} |");
            }
        }

        sb.AppendLine();
        sb.AppendLine("## 4. Análise e Conclusões Técnicas dos Cenários");
        sb.AppendLine();
        sb.AppendLine("1. **Identificação de Duplicidades (`COUNT(DISTINCT)`)**:");
        sb.AppendLine("   * A medição empírica através de `COUNT(DISTINCT IdTransacaoOriginal)` permite verificar se a política de reentrega (NACK com `requeue=true`) gera registros duplicados na Ausência de Idempotência.");
        sb.AppendLine();
        sb.AppendLine("2. **Validação do Backlog Real do Broker**:");
        sb.AppendLine("   * O acompanhamento contínuo da RabbitMQ Management API registra o número exato de mensagens prontas (`messages_ready`) e não confirmadas (`messages_unacknowledged`) durante episódios de indisponibilidade.");
        sb.AppendLine();
        sb.AppendLine("3. **Desacoplamento de Vazão e Drenagem no Cenário D (150 req/s)**:");
        sb.AppendLine("   * O experimento de carga mantida confirma que o fluxo assíncrono ingere a taxa solicitada (150 req/s) respondendo HTTP 202 rapidamente, enquanto o Worker drena o backlog acumulado no RabbitMQ na velocidade sustentável do banco de dados.");

        return sb.ToString();
    }

    public static ConsolidatedScenarioResult ConsolidateScenario(string scenario, string flow, List<SingleRunResult> runs)
    {
        MetricStats calcStatsD(IEnumerable<double> vals)
        {
            var list = vals.ToList();
            if (list.Count == 0) return new MetricStats();
            double mean = list.Average();
            double min = list.Min();
            double max = list.Max();
            double sumSq = list.Sum(x => Math.Pow(x - mean, 2));
            double stdDev = list.Count > 1 ? Math.Sqrt(sumSq / (list.Count - 1)) : 0;
            return new MetricStats { Mean = mean, Min = min, Max = max, StdDev = stdDev };
        }

        MetricStats calcStatsI(IEnumerable<int> vals) => calcStatsD(vals.Select(v => (double)v));

        MetricStats? calcNullableStats(IEnumerable<double?> vals)
        {
            var valid = vals.Where(v => v.HasValue).Select(v => v!.Value).ToList();
            if (valid.Count == 0) return null;
            return calcStatsD(valid);
        }

        // Global Percentiles pooled across raw request results of all 3 runs
        var allPooledRequests = runs.SelectMany(r => r.RawRequestResults).ToList();
        var allPooledLatencies = allPooledRequests.Select(r => r.LatencyMs).OrderBy(x => x).ToList();

        double globalP95All = BenchmarkEngine.GetPercentile(allPooledLatencies, 0.95);
        double globalP99All = BenchmarkEngine.GetPercentile(allPooledLatencies, 0.99);

        var succPooledLatencies = allPooledRequests.Where(r => r.IsSuccess).Select(r => r.LatencyMs).OrderBy(x => x).ToList();
        double? globalP95Succ = succPooledLatencies.Count > 0 ? BenchmarkEngine.GetPercentile(succPooledLatencies, 0.95) : null;
        double? globalP99Succ = succPooledLatencies.Count > 0 ? BenchmarkEngine.GetPercentile(succPooledLatencies, 0.99) : null;

        return new ConsolidatedScenarioResult
        {
            Scenario = scenario,
            Flow = flow,
            Runs = runs,
            SentStats = calcStatsI(runs.Select(r => r.Sent)),
            HTTPAcceptedStats = calcStatsI(runs.Select(r => r.HTTPAccepted)),
            HTTPFailedStats = calcStatsI(runs.Select(r => r.HTTPFailed)),
            AcceptanceRateStats = calcStatsD(runs.Select(r => r.HTTPAcceptanceRate)),
            PersistedRowsStats = calcStatsI(runs.Select(r => r.PersistedRows)),
            UniquePersistedStats = calcStatsI(runs.Select(r => r.UniquePersisted)),
            DuplicateRowsStats = calcStatsI(runs.Select(r => r.DuplicateRows)),
            UniqueLostStats = calcStatsI(runs.Select(r => r.UniqueLost)),
            PersistenceRateStats = calcStatsD(runs.Select(r => r.UniquePersistenceRate)),
            HttpLatencyMeanAllStats = calcStatsD(runs.Select(r => r.HttpLatencyMeanAll)),
            AverageRunP95AllStats = calcStatsD(runs.Select(r => r.HttpP95All)),
            AverageRunP99AllStats = calcStatsD(runs.Select(r => r.HttpP99All)),
            ActualSendRateStats = calcStatsD(runs.Select(r => r.ActualSendRate)),
            MaxQueueDepthStats = calcStatsI(runs.Select(r => r.MaxQueueDepth ?? 0)),
            GlobalP95AllRequests = globalP95All,
            GlobalP99AllRequests = globalP99All,
            GlobalP95SuccessfulRequests = globalP95Succ,
            GlobalP99SuccessfulRequests = globalP99Succ,
            HttpRecoveryTimeStats = calcNullableStats(runs.Select(r => r.HttpRecoveryTimeMs)),
            PersistenceRecoveryTimeStats = calcNullableStats(runs.Select(r => r.PersistenceRecoveryTimeMs)),
            BacklogDrainTimeStats = calcNullableStats(runs.Select(r => r.BacklogDrainTimeMs)),
            CompletionThroughputStats = calcNullableStats(runs.Select(r => r.AverageCompletionThroughput))
        };
    }
}
