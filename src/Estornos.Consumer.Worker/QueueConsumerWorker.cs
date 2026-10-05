using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Estornos.Consumer.Worker.Data;
using Estornos.Consumer.Worker.Data.Models;

namespace Estornos.Consumer.Worker;

public class QueueConsumerWorker : BackgroundService
{
    private readonly ILogger<QueueConsumerWorker> _logger;
    private readonly IServiceProvider _serviceProvider;
    private IConnection? _connection;
    private IModel? _channel;
    private readonly string _rabbitHost;

    public QueueConsumerWorker(ILogger<QueueConsumerWorker> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        stoppingToken.Register(() =>
        {
            _logger.LogInformation("Parando o consumidor do RabbitMQ...");
            _channel?.Close();
            _connection?.Close();
        });

        // Loop de conexão resiliente para o RabbitMQ
        int retryCount = 0;
        int maxRetries = 15;
        while (retryCount < maxRetries && _connection == null && !stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Tentando conectar ao RabbitMQ no host '{Host}' (Tentativa {Attempt} de {MaxRetries})...", _rabbitHost, retryCount + 1, maxRetries);
                var factory = new ConnectionFactory() { HostName = _rabbitHost };
                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();
                _channel.QueueDeclare(queue: "estornos-queue",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);
                
                _channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                _logger.LogInformation("Conectado ao RabbitMQ com sucesso!");
            }
            catch (Exception ex)
            {
                retryCount++;
                _logger.LogWarning("RabbitMQ ainda não está pronto: {Message}. Aguardando 5 segundos...", ex.Message);
                Thread.Sleep(5000);
            }
        }

        if (_connection == null || _channel == null)
        {
            _logger.LogCritical("Não foi possível conectar ao RabbitMQ. Encerrando worker.");
            throw new Exception("Falha fatal na conexão com o RabbitMQ");
        }

        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            _logger.LogInformation("[CONSUMER] Mensagem recebida da fila: {Message}", message);

            try
            {
                var request = JsonSerializer.Deserialize<RequestEstorno>(message);
                if (request != null)
                {
                    using (var scope = _serviceProvider.CreateScope())
                    {
                        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                        var estorno = new Estorno
                        {
                            Id = Guid.NewGuid(),
                            IdTransacaoOriginal = request.IdTransacaoOriginal,
                            Valor = request.Valor,
                            Motivo = request.Motivo ?? string.Empty,
                            Status = "Processado", // Sincronizado no banco pelo Worker
                            DataCriacao = DateTime.UtcNow
                        };

                        context.Estornos.Add(estorno);
                        await context.SaveChangesAsync();
                        _logger.LogInformation("[CONSUMER] Estorno gravado no banco de dados para TX: {TxId}", request.IdTransacaoOriginal);
                    }
                }

                // Confirma o recebimento e processamento
                try
                {
                    if (_channel != null && _channel.IsOpen)
                    {
                        _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                }
                catch (AlreadyClosedException) { }
                catch (Exception ackEx)
                {
                    _logger.LogWarning(ackEx, "[CONSUMER] Falha ao enviar BasicAck (canal/conexão encerrados).");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[CONSUMER] Falha ao gravar no banco. Reenfileirando mensagem no RabbitMQ...");
                // Se der erro (ex: banco fora), rejeita a mensagem com requeue=true para tentar novamente quando o banco voltar
                try
                {
                    if (_channel != null && _channel.IsOpen)
                    {
                        _channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                    }
                }
                catch (AlreadyClosedException) { }
                catch (Exception nackEx)
                {
                    _logger.LogWarning(nackEx, "[CONSUMER] Falha ao enviar BasicNack (canal/conexão encerrados).");
                }
                
                // Pausa breve para evitar loop rápido de erro em caso de banco offline
                await Task.Delay(2000, stoppingToken);
            }
        };

        _channel.BasicConsume(queue: "estornos-queue",
                             autoAck: false, // Confirmação manual
                             consumer: consumer);

        return Task.CompletedTask;
    }
}

public record RequestEstorno(string IdTransacaoOriginal, decimal Valor, string Motivo);
