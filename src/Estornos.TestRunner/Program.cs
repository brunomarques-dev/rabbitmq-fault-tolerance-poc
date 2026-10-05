using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Estornos.TestRunner;

class Program
{
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
        Console.WriteLine("        TEST RUNNER RESTRUTURADO DE RESILIÊNCIA E BENCHMARK (TCC)");
        Console.WriteLine("========================================================================");
        Console.ResetColor();
        Console.WriteLine($"Diretório do Projeto: {workspacePath}");
        Console.WriteLine("------------------------------------------------------------------------");

        int argIndex = 0;
        while (true)
        {
            string choice;
            if (args != null && argIndex < args.Length)
            {
                choice = args[argIndex++];
                Console.WriteLine($"\n[CLI Argumento] Opção selecionada: {choice}");
            }
            else
            {
                Console.WriteLine("\nSelecione uma opção:");
                Console.WriteLine("1) [Cenário A] - Falha de Aplicação (10 req/s - 3 rodadas Sinc/Asinc)");
                Console.WriteLine("2) [Cenário B] - Falha de Banco de Dados (10 req/s - 3 rodadas Sinc/Asinc)");
                Console.WriteLine("3) [Cenário C] - Controle (10 req/s - 3 rodadas Sinc/Asinc)");
                Console.WriteLine("4) [Cenário D] - Carga Elevada (150 req/s - 3 rodadas Sinc/Asinc)");
                Console.WriteLine("5) [Cenário E] - Falha de Mensageria (10 req/s - 3 rodadas Sinc/Asinc)");
                Console.WriteLine("6) [EXECUTAR CAMPANHA COMPLETA] - Executa Cenários C, A, B, E e D (3 rodadas cada)");
                Console.WriteLine("7) [SMOKE TEST DE VALIDAÇÃO] - Valida infra, warm-up, purge, COUNT DISTINCT e escrita sem rodar o benchmark longo");
                Console.WriteLine("9) [CALIBRAÇÃO DA TAXA-BASE] - Executa testes curtos (3, 5 e 7 req/s por 120s) em Controle Assíncrono");
                Console.WriteLine("8) Sair");
                Console.Write("\nOpção: ");

                string? rawChoice = Console.ReadLine();
                if (rawChoice == null) return;
                choice = new string(rawChoice.Where(char.IsAsciiLetterOrDigit).ToArray());
                if (string.IsNullOrEmpty(choice)) return;
            }

            string sessionDirName = $"{DateTime.Now:yyyy-MM-dd_HH-mm}";

            try
            {
                switch (choice)
                {
                    case "1":
                    {
                        await DockerManager.RunWarmUpAsync(workspacePath);
                        var resA = await ScenarioRunner.RunScenarioA(workspacePath);
                        await ReportGenerator.SaveSessionResultsAsync(workspacePath, sessionDirName, resA);
                        break;
                    }

                    case "2":
                    {
                        await DockerManager.RunWarmUpAsync(workspacePath);
                        var resB = await ScenarioRunner.RunScenarioB(workspacePath);
                        await ReportGenerator.SaveSessionResultsAsync(workspacePath, sessionDirName, resB);
                        break;
                    }

                    case "3":
                    {
                        await DockerManager.RunWarmUpAsync(workspacePath);
                        var resC = await ScenarioRunner.RunScenarioC(workspacePath);
                        await ReportGenerator.SaveSessionResultsAsync(workspacePath, sessionDirName, resC);
                        break;
                    }

                    case "4":
                    {
                        await DockerManager.RunWarmUpAsync(workspacePath);
                        var resD = await ScenarioRunner.RunScenarioD(workspacePath);
                        await ReportGenerator.SaveSessionResultsAsync(workspacePath, sessionDirName, resD);
                        break;
                    }

                    case "5":
                    {
                        string eSessionDirName = $"corrected-scenario-e_{DateTime.Now:yyyy-MM-dd_HH-mm-ss}";
                        await DockerManager.RunWarmUpAsync(workspacePath);
                        var resE = await ScenarioRunner.RunScenarioE(workspacePath);
                        await ReportGenerator.SaveSessionResultsAsync(workspacePath, eSessionDirName, resE);
                        break;
                    }

                    case "6":
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"\n[INICIANDO CAMPANHA COMPLETA DOS 5 CENÁRIOS (3 RODADAS CADA)]");
                        Console.ResetColor();

                        await DockerManager.RunWarmUpAsync(workspacePath);

                        var fullCampaign = new List<ConsolidatedScenarioResult>();
                        fullCampaign.AddRange(await ScenarioRunner.RunScenarioC(workspacePath));
                        fullCampaign.AddRange(await ScenarioRunner.RunScenarioA(workspacePath));
                        fullCampaign.AddRange(await ScenarioRunner.RunScenarioB(workspacePath));
                        fullCampaign.AddRange(await ScenarioRunner.RunScenarioE(workspacePath));
                        fullCampaign.AddRange(await ScenarioRunner.RunScenarioD(workspacePath));

                        await ReportGenerator.SaveSessionResultsAsync(workspacePath, sessionDirName, fullCampaign);
                        break;
                    }

                    case "7":
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("\n=== [EXECUTANDO SMOKE TEST DE VALIDAÇÃO TECNICA] ===");
                        Console.ResetColor();

                        await DockerManager.PrepareCleanEnvironmentAsync(workspacePath);
                        await DockerManager.RunWarmUpAsync(workspacePath);

                        Console.WriteLine("[SMOKE TEST] Rodando 1 min de teste curto (10 req/s em Sinc e Asinc)...");
                        var smokeC = await ScenarioRunner.RunScenarioC(workspacePath, numRuns: 1);

                        string smokeSession = $"smoke-test-{DateTime.Now:yyyy-MM-dd_HH-mm}";
                        await ReportGenerator.SaveSessionResultsAsync(workspacePath, smokeSession, smokeC);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("\n[SMOKE TEST CONCLUÍDO COM SUCESSO]");
                        Console.WriteLine($"Relatório gerado em: results/{smokeSession}/summary.md");
                        Console.ResetColor();
                        break;
                    }

                    case "8":
                        Console.WriteLine("Encerrando TestRunner.");
                        return;

                    case "9":
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("\n=== [EXECUTANDO CALIBRAÇÃO DE TAXA-BASE (3, 5 E 7 REQ/S POR 120S)] ===");
                        Console.ResetColor();

                        await ScenarioRunner.RunRateCalibrationAsync(workspacePath);
                        break;
                    }

                    case "10":
                    {
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine("\n=== [EXECUTANDO TESTE DIAGNÓSTICO DO CENÁRIO E (5 REPETIÇÕES)] ===");
                        Console.ResetColor();

                        await DiagnosticRunner.RunDiagnosticCampaignAsync(workspacePath, 5);
                        break;
                    }

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
