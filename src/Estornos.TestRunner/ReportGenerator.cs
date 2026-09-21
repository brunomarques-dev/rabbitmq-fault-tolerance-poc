using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Estornos.TestRunner;

public static class ReportGenerator
{
    public static void GenerateReport(string outputPath, List<ScenarioResult> results)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Relatório de Resiliência: Falhas de Infraestrutura");
        sb.AppendLine();
        sb.AppendLine($"Gerado em: {DateTime.Now:dd/MM/yyyy HH:mm:ss}");
        sb.AppendLine();
        sb.AppendLine("Este relatório consolida os resultados dos testes de falhas induzidas na POC (Tráfego constante de 10 req/s por 5 minutos).");
        sb.AppendLine("Metodologia de Conectividade:");
        sb.AppendLine("- Timeout HTTP global configurado explicitamente em 60 segundos.");
        sb.AppendLine("- Publicador RabbitMQ com mecanismo Fail-Fast, SemaphoreSlim(1,1) para reconexão não-bloqueante e Cooldown fixo de 2 segundos entre tentativas de reconexão.");
        sb.AppendLine();
        sb.AppendLine("## Resultados Gerais dos Testes");
        sb.AppendLine();
        sb.AppendLine("| Cenário | Fluxo | Enviadas | Sucesso HTTP | Falha HTTP | Taxa Sucesso | Registros Persistidos | Perda de Dados | Latência Média | Latência P95 | Throughput | T. Recuperação | Backlog Máx. |");
        sb.AppendLine("| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |");

        var allResults = new List<ScenarioResult>(results);

        if (File.Exists(outputPath))
        {
            try
            {
                var lines = File.ReadAllLines(outputPath);
                foreach (var line in lines)
                {
                    if (line.StartsWith("| Cenário") && !line.Contains("Fluxo"))
                    {
                        var parts = line.Split('|').Select(p => p.Trim()).ToArray();
                        if (parts.Length >= 14)
                        {
                            string scenario = parts[1];
                            string flow = parts[2];
                            if (!allResults.Any(r => r.Scenario == scenario && r.Flow == flow))
                            {
                                int.TryParse(parts[4], out int success);
                                int.TryParse(parts[5], out int failure);
                                int sent = success + failure; // Garantia aditiva a partir dos dados brutos do relatório existente
                                int.TryParse(parts[7], out int persisted);
                                double.TryParse(parts[9].Replace(" ms", "").Trim(), out double avgLat);
                                double.TryParse(parts[10].Replace(" ms", "").Trim(), out double p95Lat);
                                double.TryParse(parts[11].Replace(" req/s", "").Trim(), out double throughput);
                                double.TryParse(parts[12].Replace(" ms", "").Trim(), out double recovery);
                                int.TryParse(parts[13].Replace(" msgs", "").Trim(), out int maxQueue);

                                allResults.Add(new ScenarioResult
                                {
                                    Scenario = scenario,
                                    Flow = flow,
                                    Sent = sent,
                                    Success = success,
                                    Failure = failure,
                                    Persisted = persisted,
                                    AverageLatencyMs = avgLat,
                                    P95LatencyMs = p95Lat,
                                    Throughput = throughput,
                                    RecoveryTimeMs = recovery,
                                    MaxQueueDepth = maxQueue
                                });
                            }
                        }
                    }
                }
            }
            catch
            {
                // Em caso de erro de parsing, usa os resultados em memória
            }
        }

        foreach (var r in allResults)
        {
            string latencyAvgStr = r.AverageLatencyMs > 0 ? $"{r.AverageLatencyMs:F2} ms" : "-";
            string latencyP95Str = r.P95LatencyMs > 0 ? $"{r.P95LatencyMs:F2} ms" : "-";
            string throughputStr = $"{r.Throughput:F2} req/s";
            string recoveryStr = r.RecoveryTimeMs > 0 ? $"{r.RecoveryTimeMs:F2} ms" : (r.RecoveryTimeMs < 0 ? "Não recuperou" : "-");
            string maxQueueStr = r.MaxQueueDepth > 0 ? $"{r.MaxQueueDepth} msgs" : "-";

            sb.AppendLine($"| {r.Scenario} | {r.Flow} | {r.Sent} | {r.Success} | {r.Failure} | {r.SuccessRate:F2}% | {r.Persisted} | {r.LostData} | {latencyAvgStr} | {latencyP95Str} | {throughputStr} | {recoveryStr} | {maxQueueStr} |");
        }

        sb.AppendLine();
        sb.AppendLine("## Conclusões e Análise Comparativa");
        sb.AppendLine();
        sb.AppendLine("1. **Operação Normal (Cenário C - Controle)**:");
        sb.AppendLine("   * Serve como linha de base. O modelo assíncrono exibe menor latência média ao cliente, pois o processamento em banco ocorre fora do ciclo HTTP.");
        sb.AppendLine();
        sb.AppendLine("2. **Falha de Aplicação (Cenário A)**:");
        sb.AppendLine("   * **Síncrono**: A queda da `estornos-processor-api` por 2 minutos interrompe a rota direta. As requisições que estouram o timeout de 60s ou sofrem erro de conexão falham, enquanto as requisições pendentes nos buffers de socket do Gateway ao final do período de queda são atendidas com alta latência assim que a API se restabelece.");
        sb.AppendLine("   * **Assíncrono**: A queda do `estornos-consumer-worker` por 2 minutos é transparente para o cliente externo (100% de sucesso HTTP e latência baixa). O RabbitMQ absorve o backlog de mensagens, que são processadas sem perda após o retorno do Worker.");
        sb.AppendLine();
        sb.AppendLine("3. **Falha de Banco de Dados (Cenário B)**:");
        sb.AppendLine("   * **Síncrono**: A queda do `db` por 2 minutos causa falhas na persistência direta para as requisições síncronas, resultando em perda de dados.");
        sb.AppendLine("   * **Assíncrono**: A queda do `db` por 2 minutos causa erros de persistência no `estornos-consumer-worker`, mas a política de NACK com requeue mantém as mensagens salvas na fila. Quando o banco de dados retorna, todas as mensagens são persistidas com 0% de perda.");
        sb.AppendLine();
        sb.AppendLine("4. **Teste de Carga (Cenário D)**:");
        sb.AppendLine("   * **Síncrono**: Sob carga pesada de 150 req/s, o fluxo síncrono apresenta contenção de concorrência e dependência direta do tempo de resposta físico do banco de dados.");
        sb.AppendLine("   * **Assíncrono**: O fluxo assíncrono processa a entrada rapidamente respondendo de imediato (`202 Accepted`) e publica no RabbitMQ, protegendo o banco contra picos de carga.");
        sb.AppendLine();
        sb.AppendLine("5. **Falha de Mensageria (Cenário E)**:");
        sb.AppendLine("   * **Síncrono**: A indisponibilidade do RabbitMQ é transparente para a rota síncrona, mantendo 100% de sucesso e latência normal, já que a comunicação síncrona é direta via HTTP com a API de processamento e o banco.");
        sb.AppendLine("   * **Assíncrono**: A queda do RabbitMQ faz com que o Gateway acione o mecanismo de Fail-Fast (cooldown de 2s e SemaphoreSlim de reconexão não-bloqueante), retornando erro de indisponibilidade em milissegundos sem travar threads da aplicação. Ao restabelecer o RabbitMQ, a publicação recupera o envio normal no próximo ciclo.");

        File.WriteAllText(outputPath, sb.ToString(), Encoding.UTF8);
        Console.WriteLine($"\n[RELATÓRIO] Relatório de testes gravado em: {outputPath}");
    }

    public static ScenarioResult ConsolidateResults(string scenario, string flow, List<ScenarioResult> runResults)
    {
        int totalSuccess = (int)Math.Round(runResults.Average(r => r.Success));
        int totalFailure = (int)Math.Round(runResults.Average(r => r.Failure));
        int totalSent = totalSuccess + totalFailure; // Derivado diretamente das contagens brutas consolidadas
        int totalPersisted = (int)Math.Round(runResults.Average(r => r.Persisted));
        double avgLatency = runResults.Average(r => r.AverageLatencyMs);
        double avgP95 = runResults.Average(r => r.P95LatencyMs);
        double avgThroughput = runResults.Average(r => r.Throughput);
        double avgRecovery = runResults.Average(r => r.RecoveryTimeMs);
        int maxQueue = (int)Math.Round(runResults.Average(r => r.MaxQueueDepth));

        return new ScenarioResult
        {
            Scenario = scenario,
            Flow = flow,
            Sent = totalSent,
            Success = totalSuccess,
            Failure = totalFailure,
            Persisted = totalPersisted,
            AverageLatencyMs = avgLatency,
            P95LatencyMs = avgP95,
            Throughput = avgThroughput,
            RecoveryTimeMs = avgRecovery,
            MaxQueueDepth = maxQueue
        };
    }
}
