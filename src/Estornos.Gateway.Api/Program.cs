using Estornos.Gateway.Api.Models;
using Estornos.Gateway.Api.Services;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHttpClient("ProcessorClient", client =>
{
    client.Timeout = TimeSpan.FromSeconds(60); // Timeout HTTP de 60s explicitamente configurado
});

var rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
builder.Services.AddSingleton<RabbitPublisherService>(sp => new RabbitPublisherService(rabbitHost));
builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();
var processingApiUrl = Environment.GetEnvironmentVariable("PROCESSING_API_URL") ?? "http://localhost:5001";

// 1. FLUXO SÍNCRONO: Gateway -> Processor -> DB
app.MapPost("/api/estornos/sinc", async (RequestEstorno request, IHttpClientFactory httpClientFactory) =>
{
    if (string.IsNullOrWhiteSpace(request.IdTransacaoOriginal))
        return Results.BadRequest(new { mensagem = "O ID da transação original é obrigatório." });
    if (request.Valor <= 0)
        return Results.BadRequest(new { mensagem = "O valor do estorno deve ser maior que zero." });

    var httpClient = httpClientFactory.CreateClient("ProcessorClient");
    var content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");
    try
    {
        var response = await httpClient.PostAsync($"{processingApiUrl}/api/internal/estornos", content);
        var responseBody = await response.Content.ReadAsStringAsync();

        if (response.IsSuccessStatusCode)
            return Results.Json(JsonSerializer.Deserialize<object>(responseBody), statusCode: (int)response.StatusCode);

        return Results.Json(new { erro = "Falha no processamento síncrono", detalhes = responseBody }, statusCode: (int)response.StatusCode);
    }
    catch (Exception ex)
    {
        return Results.Json(new { erro = "Serviço de Processamento indisponível", mensagem = ex.Message }, statusCode: 500);
    }
});

// 2. FLUXO ASSÍNCRONO: Gateway -> RabbitMQ -> 202 Accepted
app.MapPost("/api/estornos/asinc", (RequestEstorno request, RabbitPublisherService publisher) =>
{
    if (string.IsNullOrWhiteSpace(request.IdTransacaoOriginal))
        return Results.BadRequest(new { mensagem = "O ID da transação original é obrigatório." });
    if (request.Valor <= 0)
        return Results.BadRequest(new { mensagem = "O valor do estorno deve ser maior que zero." });

    try
    {
        publisher.Publish(request);
        return Results.Accepted(string.Empty, new { mensagem = "Encaminhado para a fila com sucesso (Assíncrono)." });
    }
    catch (Exception ex)
    {
        return Results.Json(new { erro = "Fila de mensageria indisponível", mensagem = ex.Message }, statusCode: 500);
    }
});

app.Run();
