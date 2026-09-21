using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Estornos.TestRunner;

class Program
{
    private static readonly List<ScenarioResult> Results = new();

    static async Task Main(string[] args)
    {
        Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });

        string currentDir = AppDomain.CurrentDomain.BaseDirectory;
        string workspacePath = currentDir;
        while (!string.IsNullOrEmpty(workspacePath) && !File.Exists(Path.Combine(workspacePath, "docker-compose.yml")))
        {
            workspacePath = Path.GetDirectoryName(workspacePath) ?? "";
        }

        if (string.IsNullOrEmpty(workspacePath))
        {
            workspacePath = @"c:\Users\BrunoAAM\Documents\REPOS\PocMicrosservicos\API SINC";
        }

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("========================================================================");
        Console.WriteLine("             TEST RUNNER DE RESILIÊNCIA E BENCHMARK");
        Console.WriteLine("========================================================================");
        Console.ResetColor();
        Console.WriteLine($"Diretório do Projeto: {workspacePath}");
        Console.WriteLine("------------------------------------------------------------------------");

        string reportPath = Path.Combine(workspacePath, "test_results.md");

        while (true)
        {
            Console.WriteLine("\nSelecione uma opção de teste:");
            Console.WriteLine("1) [Cenário A] - Falha de Aplicação (API/Worker offline por 2 min)");
            Console.WriteLine("2) [Cenário B] - Falha de Banco de Dados (Banco offline por 2 min)");
            Console.WriteLine("3) [Cenário C] - Operação Normal (Controle - 5 minutos sem quedas)");
            Console.WriteLine("4) [Cenário D] - Teste de Carga (150 req/s por 5 min)");
            Console.WriteLine("5) [Cenário E] - Falha de Mensageria (RabbitMQ offline por 2 min)");
            Console.WriteLine("6) [Executar Todos] - Executa Cenários C, A, B, E e D sequencialmente");
            Console.WriteLine("7) [Gerar Relatório] - Gravar resultados acumulados no test_results.md");
            Console.WriteLine("8) Sair");
            Console.Write("\nOpção: ");

            string? rawChoice = Console.ReadLine();
            if (rawChoice == null) return;
            string choice = new string(rawChoice.Where(char.IsAsciiLetterOrDigit).ToArray());
            if (string.IsNullOrEmpty(choice)) return;

            try
            {
                switch (choice)
                {
                    case "1":
                        await ScenarioRunner.RunScenarioA(workspacePath, Results);
                        ReportGenerator.GenerateReport(reportPath, Results);
                        break;

                    case "2":
                        await ScenarioRunner.RunScenarioB(workspacePath, Results);
                        ReportGenerator.GenerateReport(reportPath, Results);
                        break;

                    case "3":
                        await ScenarioRunner.RunScenarioC(workspacePath, Results);
                        ReportGenerator.GenerateReport(reportPath, Results);
                        break;

                    case "4":
                        await ScenarioRunner.RunScenarioD(workspacePath, Results);
                        ReportGenerator.GenerateReport(reportPath, Results);
                        break;

                    case "5":
                        await ScenarioRunner.RunScenarioE(workspacePath, Results);
                        ReportGenerator.GenerateReport(reportPath, Results);
                        break;

                    case "6":
                        await ScenarioRunner.RunScenarioC(workspacePath, Results);
                        await ScenarioRunner.RunScenarioA(workspacePath, Results);
                        await ScenarioRunner.RunScenarioB(workspacePath, Results);
                        await ScenarioRunner.RunScenarioE(workspacePath, Results);
                        await ScenarioRunner.RunScenarioD(workspacePath, Results);
                        ReportGenerator.GenerateReport(reportPath, Results);
                        break;

                    case "7":
                        ReportGenerator.GenerateReport(reportPath, Results);
                        break;

                    case "8":
                        Console.WriteLine("Encerrando TestRunner.");
                        return;

                    default:
                        Console.WriteLine("Opção inválida.");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\n[ERRO NA EXECUÇÃO]: {ex.Message}");
                Console.ResetColor();
            }
        }
    }
}
