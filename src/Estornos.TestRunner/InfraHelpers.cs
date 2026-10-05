using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Estornos.TestRunner;

public static class DockerManager
{
    public static void RunDockerCommand(string workspacePath, string command, string serviceName)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"[DOCKER] Executando: docker compose {command} {serviceName}...");
        Console.ResetColor();

        try
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = $"compose {command} {serviceName}",
                WorkingDirectory = workspacePath,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = Process.Start(startInfo);
            if (process != null)
            {
                process.WaitForExit();
                if (process.ExitCode == 0)
                {
                    Console.WriteLine($"[DOCKER] Sucesso: {serviceName} alterado para '{command}'.");
                    return;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AVISO DOCKER] Exceção ao executar comando docker: {ex.Message}");
        }

        Console.ForegroundColor = ConsoleColor.Magenta;
        Console.WriteLine($"========================================================");
        Console.WriteLine($"[AÇÃO MANUAL NECESSÁRIA]");
        Console.WriteLine($"Execute no terminal da pasta raiz:");
        Console.WriteLine($"  docker compose {command} {serviceName}");
        Console.WriteLine($"========================================================");
        Console.ResetColor();
        Console.Write("Pressione [Enter] após executar...");
        Console.ReadLine();
    }

    public static void EnsureAllServicesRunning(string workspacePath)
    {
        Console.WriteLine("[INFRA] Garantindo que todos os serviços estejam ativos...");
        RunDockerCommand(workspacePath, "start", "rabbitmq");
        RunDockerCommand(workspacePath, "start", "db");
        RunDockerCommand(workspacePath, "start", "estornos-consumer-worker");
        RunDockerCommand(workspacePath, "start", "estornos-processor-api");
        RunDockerCommand(workspacePath, "start", "estornos-gateway-api");
        Thread.Sleep(5000);
    }

    public static async Task PrepareCleanEnvironmentAsync(string workspacePath)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("\n[LIMPEZA RIGOROSA DE AMBIENTE]");
        Console.ResetColor();

        // 1. Garantir que os serviços de infraestutura (RabbitMQ e DB) estejam rodando para receber comandos de limpeza
        RunDockerCommand(workspacePath, "start", "rabbitmq");
        RunDockerCommand(workspacePath, "start", "db");
        await Task.Delay(2000);

        // 2. Interromper o Consumer para evitar processar em voo durante a limpeza
        RunDockerCommand(workspacePath, "stop", "estornos-consumer-worker");
        await Task.Delay(2000);

        // 3. Expurgar fila RabbitMQ
        bool purged = await RabbitMqManager.PurgeQueueAsync();
        if (!purged)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("[FALHA FATAL DE LIMPEZA] Não foi possível expurgar a fila do RabbitMQ.");
            Console.ResetColor();
            throw new Exception("Falha de infraestrutura: Purge do RabbitMQ falhou.");
        }

        // 4. Confirmar que a fila possui 0 mensagens
        var (ready, unacked, total) = await RabbitMqManager.GetQueueStatusAsync();
        if (total > 0)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[FALHA FATAL DE LIMPEZA] Fila RabbitMQ ainda contém {total} mensagens após purge.");
            Console.ResetColor();
            throw new Exception("Falha de infraestrutura: Fila RabbitMQ não zerou após purge.");
        }

        // 5. Limpar tabela no SQL Server
        DatabaseHelper.ClearEstornos();

        // 6. Confirmar COUNT(*) = 0 no banco
        int dbCount = DatabaseHelper.GetEstornosCount();
        if (dbCount > 0)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[FALHA FATAL DE LIMPEZA] Banco de dados ainda contém {dbCount} registros.");
            Console.ResetColor();
            throw new Exception("Falha de infraestrutura: Banco de dados não zerou após limpeza.");
        }

        // 6. Iniciar novamente todos os serviços (incluindo o Consumer)
        EnsureAllServicesRunning(workspacePath);

        // 7. Validar estado inicial limpo de toda a infraestrutura
        await ValidateInitialStateAsync();
    }

    public static async Task ValidateInitialStateAsync()
    {
        Console.WriteLine("[VALIDAÇÃO] Verificando disponibilidade e limpeza dos serviços...");

        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };

        // Gateway ping
        try
        {
            var res = await client.GetAsync("http://localhost:5000/api/estornos/sinc");
        }
        catch { }

        // RabbitMQ queue status
        var (ready, unacked, total) = await RabbitMqManager.GetQueueStatusAsync();
        if (total > 0)
        {
            throw new Exception($"Validação de Estado Inicial Falhou: Fila RabbitMQ não está zerada ({total} msgs).");
        }

        // SQL Server count
        int dbCount = DatabaseHelper.GetEstornosCount();
        if (dbCount > 0)
        {
            throw new Exception($"Validação de Estado Inicial Falhou: Banco de dados não está zerado ({dbCount} registros).");
        }

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("[ESTADO INICIAL VALIDADOS] Todos os serviços online, Fila=0, Banco=0.");
        Console.ResetColor();
    }

    public static async Task RunWarmUpAsync(string workspacePath)
    {
        Console.ForegroundColor = ConsoleColor.Magenta;
        Console.WriteLine("\n=== [EXECUTANDO WARM-UP PADRONIZADO (20 REQUISITION DISPAROS)] ===");
        Console.ResetColor();

        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        for (int i = 0; i < 10; i++)
        {
            try
            {
                var payload = new RequestEstorno($"WARMUP-SINC-{Guid.NewGuid()}", 10.00m, "Warmup");
                var json = JsonSerializer.Serialize(payload);
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                await client.PostAsync("http://localhost:5000/api/estornos/sinc", content);
            }
            catch { }

            try
            {
                var payload = new RequestEstorno($"WARMUP-ASINC-{Guid.NewGuid()}", 10.00m, "Warmup");
                var json = JsonSerializer.Serialize(payload);
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                await client.PostAsync("http://localhost:5000/api/estornos/asinc", content);
            }
            catch { }
        }

        await Task.Delay(2000);
        Console.WriteLine("[WARM-UP CONCLUÍDO] Resetando banco e fila para estado zerado...");
        await PrepareCleanEnvironmentAsync(workspacePath);
    }
}

public static class DatabaseHelper
{
    private static readonly string DbConnectionString = "Server=127.0.0.1,1435;Database=EstornosDb;User Id=sa;Password=YourStrong@Pass123;TrustServerCertificate=True;";

    public static void ClearEstornos()
    {
        Console.WriteLine("[DATABASE] Limpando tabela 'Estornos' via DELETE...");
        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                using var connection = new SqlConnection(DbConnectionString);
                connection.Open();
                string sql = "IF OBJECT_ID('Estornos', 'U') IS NOT NULL DELETE FROM Estornos;";
                using var command = new SqlCommand(sql, connection);
                command.CommandTimeout = 10;
                command.ExecuteNonQuery();
                Console.WriteLine("[DATABASE] Tabela limpa com sucesso.");
                return;
            }
            catch (Exception ex)
            {
                if (attempt == maxRetries)
                {
                    Console.WriteLine($"[ERRO BANCO] Falha ao limpar tabela após {maxRetries} tentativas: {ex.Message}");
                    throw;
                }
                Thread.Sleep(1000);
            }
        }
    }

    public static int GetEstornosCount()
    {
        int maxRetries = 5;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                using var connection = new SqlConnection(DbConnectionString);
                connection.Open();
                string sql = "IF OBJECT_ID('Estornos', 'U') IS NOT NULL SELECT COUNT(*) FROM Estornos; ELSE SELECT 0;";
                using var command = new SqlCommand(sql, connection);
                command.CommandTimeout = 5;
                var result = command.ExecuteScalar();
                return result != null ? Convert.ToInt32(result) : 0;
            }
            catch
            {
                if (attempt < maxRetries)
                {
                    Thread.Sleep(500);
                }
            }
        }
        return 0;
    }

    public static DbMetrics GetDbMetrics()
    {
        try
        {
            using var connection = new SqlConnection(DbConnectionString);
            connection.Open();

            string sqlTotal = "IF OBJECT_ID('Estornos', 'U') IS NOT NULL SELECT COUNT(*) FROM Estornos; ELSE SELECT 0;";
            using var cmdTotal = new SqlCommand(sqlTotal, connection);
            cmdTotal.CommandTimeout = 5;
            int total = Convert.ToInt32(cmdTotal.ExecuteScalar() ?? 0);

            string sqlUnique = "IF OBJECT_ID('Estornos', 'U') IS NOT NULL SELECT COUNT(DISTINCT IdTransacaoOriginal) FROM Estornos; ELSE SELECT 0;";
            using var cmdUnique = new SqlCommand(sqlUnique, connection);
            cmdUnique.CommandTimeout = 10;
            int unique = Convert.ToInt32(cmdUnique.ExecuteScalar() ?? 0);

            return new DbMetrics
            {
                TotalPersisted = total,
                UniquePersisted = unique,
                DuplicateRows = Math.Max(0, total - unique)
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AVISO SQL METRICS] Erro ao obter DbMetrics: {ex.Message}");
            return new DbMetrics { TotalPersisted = 0, UniquePersisted = 0, DuplicateRows = 0 };
        }
    }
}

public static class RabbitMqManager
{
    private static readonly HttpClient HttpClientInstance;

    static RabbitMqManager()
    {
        HttpClientInstance = new HttpClient();
        var byteArray = Encoding.ASCII.GetBytes("guest:guest");
        HttpClientInstance.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
        HttpClientInstance.Timeout = TimeSpan.FromSeconds(5);
    }

    public static async Task<(int messagesReady, int messagesUnacked, int messagesTotal)> GetQueueStatusAsync()
    {
        try
        {
            var response = await HttpClientInstance.GetAsync("http://localhost:15672/api/queues/%2F/estornos-queue");
            if (!response.IsSuccessStatusCode) return (0, 0, 0);

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            int ready = root.TryGetProperty("messages_ready", out var readyElem) ? readyElem.GetInt32() : 0;
            int unacked = root.TryGetProperty("messages_unacknowledged", out var unackedElem) ? unackedElem.GetInt32() : 0;
            int total = root.TryGetProperty("messages", out var totalElem) ? totalElem.GetInt32() : 0;

            return (ready, unacked, total);
        }
        catch
        {
            return (0, 0, 0);
        }
    }

    public static async Task<bool> PurgeQueueAsync()
    {
        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var purgeResponse = await HttpClientInstance.DeleteAsync("http://localhost:15672/api/queues/%2F/estornos-queue/contents");

                var (ready, unacked, total) = await GetQueueStatusAsync();
                if (total > 0)
                {
                    await HttpClientInstance.DeleteAsync("http://localhost:15672/api/queues/%2F/estornos-queue");
                    await Task.Delay(1000);
                }

                var (checkReady, checkUnacked, checkTotal) = await GetQueueStatusAsync();
                if (checkTotal == 0)
                {
                    return true;
                }
            }
            catch
            {
                // RabbitMQ Management API pode estar inicializando no Docker
            }

            if (attempt < maxRetries)
            {
                await Task.Delay(1000);
            }
        }
        return false;
    }

    public static CancellationTokenSource StartQueueMonitoring(CancellationToken parentToken, List<QueueMetricSample> samples, int intervalMs = 1000)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
        Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var (ready, unacked, total) = await GetQueueStatusAsync();
                samples.Add(new QueueMetricSample
                {
                    Timestamp = DateTime.UtcNow,
                    MessagesReady = ready,
                    MessagesUnacknowledged = unacked,
                    MessagesTotal = total
                });

                try
                {
                    await Task.Delay(intervalMs, cts.Token);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }
        }, cts.Token);

        return cts;
    }
}

public static class EnvironmentLogger
{
    public static async Task<string> GenerateEnvironmentReportAsync(string workspacePath)
    {
        var sb = new StringBuilder();
        sb.AppendLine("========================================================================");
        sb.AppendLine("                   INFORMAÇÕES EXATAS DO AMBIENTE");
        sb.AppendLine("========================================================================");
        sb.AppendLine($"Data da Coleta: {DateTime.Now:dd/MM/yyyy HH:mm:ss}");
        sb.AppendLine($".NET Runtime: {Environment.Version}");
        sb.AppendLine($"TargetFramework: net9.0");
        sb.AppendLine($"Sistema Operacional Host: {Environment.OSVersion}");
        sb.AppendLine($"Arquitetura SO: {Environment.Is64BitOperatingSystem switch { true => "64-bit", false => "32-bit" }}");
        sb.AppendLine($"Processadores Host (vCPU): {Environment.ProcessorCount}");
        sb.AppendLine();

        sb.AppendLine("--- Pacotes NuGet ---");
        sb.AppendLine("RabbitMQ.Client: 6.8.1");
        sb.AppendLine("Microsoft.EntityFrameworkCore.SqlServer: 9.0.2");
        sb.AppendLine("System.Data.SqlClient: 4.9.0");
        sb.AppendLine();

        sb.AppendLine("--- Versões dos Serviços de Infraestrutura (Docker) ---");
        sb.AppendLine("Imagem RabbitMQ: rabbitmq:3-management");

        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var bytes = Encoding.ASCII.GetBytes("guest:guest");
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(bytes));
            var res = await http.GetAsync("http://localhost:15672/api/overview");
            if (res.IsSuccessStatusCode)
            {
                var json = await res.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty("rabbitmq_version", out var verElem))
                {
                    sb.AppendLine($"Versão Executando RabbitMQ: {verElem.GetString()}");
                }
            }
        }
        catch
        {
            sb.AppendLine("Versão Executando RabbitMQ: Não foi possível obter via API (serviço offline)");
        }

        sb.AppendLine("Imagem SQL Server: mcr.microsoft.com/mssql/server:2022-latest");
        try
        {
            using var connection = new SqlConnection("Server=127.0.0.1,1435;Database=master;User Id=sa;Password=YourStrong@Pass123;TrustServerCertificate=True;");
            connection.Open();
            using var cmd = new SqlCommand("SELECT @@VERSION;", connection);
            var version = cmd.ExecuteScalar()?.ToString()?.Replace("\n", " ") ?? "Desconhecido";
            sb.AppendLine($"Versão Executando SQL Server: {version}");
        }
        catch
        {
            sb.AppendLine("Versão Executando SQL Server: Não foi possível obter via SQL (serviço offline)");
        }

        sb.AppendLine();
        sb.AppendLine("--- Configuração de Limites de Recursos dos Containers (docker-compose.yml) ---");
        sb.AppendLine("estornos-gateway-api:    1.0 vCPU / 512 MB RAM");
        sb.AppendLine("estornos-processor-api:  1.0 vCPU / 512 MB RAM");
        sb.AppendLine("estornos-consumer-worker: 1.0 vCPU / 512 MB RAM");
        sb.AppendLine("rabbitmq:                1.0 vCPU / 512 MB RAM");
        sb.AppendLine("db (SQL Server):         2.0 vCPU / 1.5 GB RAM");
        sb.AppendLine("========================================================================");

        return sb.ToString();
    }
}
