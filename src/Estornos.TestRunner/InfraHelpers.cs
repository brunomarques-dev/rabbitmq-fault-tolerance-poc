using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Threading;

namespace Estornos.TestRunner;

public static class DockerManager
{
    public static void RunDockerCommand(string workspacePath, string command, string serviceName)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"\n[DOCKER] Executando: docker compose {command} {serviceName}...");
        Console.ResetColor();

        bool success = false;
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
                    success = true;
                    Console.WriteLine($"[DOCKER] Sucesso: {serviceName} alterado para '{command}'.");
                }
            }
        }
        catch
        {
            // Ignora exceção e solicita intervenção manual se falhar
        }

        if (!success)
        {
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"\n========================================================");
            Console.WriteLine($"[AÇÃO MANUAL NECESSÁRIA]");
            Console.WriteLine($"Execute no terminal da pasta raiz:");
            Console.WriteLine($"  docker compose {command} {serviceName}");
            Console.WriteLine($"========================================================");
            Console.ResetColor();
            Console.Write("Pressione [Enter] após executar...");
            Console.ReadLine();
        }
    }

    public static void EnsureAllServicesRunning(string workspacePath)
    {
        Console.WriteLine("\n[INFRA] Garantindo que todos os servicos estao ativos...");
        RunDockerCommand(workspacePath, "start", "rabbitmq");
        RunDockerCommand(workspacePath, "start", "db");
        RunDockerCommand(workspacePath, "start", "estornos-consumer-worker");
        RunDockerCommand(workspacePath, "start", "estornos-processor-api");
        RunDockerCommand(workspacePath, "start", "estornos-gateway-api");
        Thread.Sleep(5000);
    }
}

public static class DatabaseHelper
{
    private static readonly string DbConnectionString = "Server=127.0.0.1,1435;Database=EstornosDb;User Id=sa;Password=YourStrong@Pass123;TrustServerCertificate=True;";

    public static void ClearEstornos()
    {
        Console.WriteLine("[DATABASE] Limpando tabela 'Estornos' e fila 'estornos-queue'...");
        try
        {
            // Purga a fila estornos-queue via RabbitMQ HTTP Management API se disponível
            using var httpClient = new HttpClient();
            var byteArray = Encoding.ASCII.GetBytes("guest:guest");
            httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
            _ = httpClient.DeleteAsync("http://localhost:15672/api/queues/%2F/estornos-queue/contents").GetAwaiter().GetResult();
        }
        catch
        {
            // Ignora se RabbitMQ estiver temporariamente offline
        }

        try
        {
            using var connection = new SqlConnection(DbConnectionString);
            connection.Open();
            string sql = "IF OBJECT_ID('Estornos', 'U') IS NOT NULL DELETE FROM Estornos;";
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 5;
            command.ExecuteNonQuery();
            Console.WriteLine("[DATABASE/RABBITMQ] Tabela e fila limpas com sucesso.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AVISO BANCO] Erro ao limpar: {ex.Message}");
        }
    }

    public static int GetEstornosCount()
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
        catch (Exception ex)
        {
            throw new Exception($"Erro de conexao SQL: {ex.Message}");
        }
    }
}
