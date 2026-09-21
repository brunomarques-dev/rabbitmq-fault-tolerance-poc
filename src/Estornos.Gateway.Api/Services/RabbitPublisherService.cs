using Estornos.Gateway.Api.Models;
using RabbitMQ.Client;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace Estornos.Gateway.Api.Services;

public class RabbitPublisherService
{
    private IConnection? _connection;
    private IModel? _channel;
    private readonly string _rabbitHost;
    private readonly SemaphoreSlim _reconnectSemaphore = new(1, 1);
    private DateTime _lastConnectionAttempt = DateTime.MinValue;
    private static readonly TimeSpan CooldownPeriod = TimeSpan.FromSeconds(2);

    public RabbitPublisherService(string rabbitHost)
    {
        _rabbitHost = rabbitHost;
    }

    private bool IsConnected()
    {
        return _connection != null && _connection.IsOpen && _channel != null && _channel.IsOpen;
    }

    private void EnsureConnection()
    {
        if (IsConnected())
            return;

        // Fail-fast se o cooldown fixo de 2s ainda não expirou
        if (DateTime.UtcNow - _lastConnectionAttempt < CooldownPeriod)
        {
            throw new Exception("Broker de mensageria indisponível (cooldown de reconexão de 2s ativo).");
        }

        // Tenta adquirir o semáforo de forma não bloqueante (apenas 1 tentativa de reconexão por vez)
        if (!_reconnectSemaphore.Wait(0))
        {
            throw new Exception("Broker de mensageria indisponível (reconexão em andamento por outra requisição).");
        }

        try
        {
            if (IsConnected())
                return;

            if (DateTime.UtcNow - _lastConnectionAttempt < CooldownPeriod)
            {
                throw new Exception("Broker de mensageria indisponível (cooldown de reconexão ativo).");
            }

            _lastConnectionAttempt = DateTime.UtcNow;

            var factory = new ConnectionFactory()
            {
                HostName = _rabbitHost,
                RequestedConnectionTimeout = TimeSpan.FromSeconds(2)
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
            _channel.QueueDeclare(queue: "estornos-queue", durable: true, exclusive: false, autoDelete: false);
        }
        finally
        {
            _reconnectSemaphore.Release();
        }
    }

    public void Publish(RequestEstorno request)
    {
        EnsureConnection();

        var messageJson = JsonSerializer.Serialize(request);
        var body = Encoding.UTF8.GetBytes(messageJson);

        var properties = _channel!.CreateBasicProperties();
        properties.Persistent = true;

        lock (_channel)
        {
            _channel.BasicPublish(exchange: "", routingKey: "estornos-queue", basicProperties: properties, body: body);
        }
    }
}

