using Estornos.Processor.Api.Data;
using Estornos.Processor.Api.Data.Models;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Configura o DbContext com SQL Server
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(connectionString));

builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

// Inicialização automática do banco de dados com retentativas (resiliência para o Docker)
using (var scope = app.Services.CreateScope())
{
    var services = scope.ServiceProvider;
    var logger = services.GetRequiredService<ILogger<Program>>();
    var context = services.GetRequiredService<AppDbContext>();

    int retryCount = 0;
    int maxRetries = 15;
    bool dbCreated = false;

    while (retryCount < maxRetries && !dbCreated)
    {
        try
        {
            logger.LogInformation("Tentando conectar ao banco de dados SQL Server (Tentativa {Attempt} de {MaxRetries})...", retryCount + 1, maxRetries);
            context.Database.EnsureCreated();
            dbCreated = true;
            logger.LogInformation("Banco de dados inicializado e verificado com sucesso!");
        }
        catch (Exception ex)
        {
            retryCount++;
            logger.LogWarning("SQL Server ainda não está pronto: {Message}. Aguardando 5 segundos antes da próxima tentativa...", ex.Message);
            Thread.Sleep(5000);
        }
    }

    if (!dbCreated)
    {
        logger.LogCritical("Não foi possível conectar ao SQL Server após várias tentativas. Encerrando aplicação.");
        throw new Exception("Falha fatal na inicialização do Banco de Dados");
    }
}

// Endpoint de POST interno para receber e atualizar o estorno
app.MapPost("/api/internal/estornos", async (RequestEstorno request, AppDbContext context) =>
{
    if (string.IsNullOrWhiteSpace(request.IdTransacaoOriginal))
    {
        return Results.BadRequest(new { mensagem = "O ID da transação original é obrigatório." });
    }

    if (request.Valor <= 0)
    {
        return Results.BadRequest(new { mensagem = "O valor do estorno deve ser maior que zero." });
    }

    var estorno = new Estorno
    {
        Id = Guid.NewGuid(),
        IdTransacaoOriginal = request.IdTransacaoOriginal,
        Valor = request.Valor,
        Motivo = request.Motivo ?? string.Empty,
        Status = "Processado", // Síncrono imediato para esta POC
        DataCriacao = DateTime.UtcNow
    };

    context.Estornos.Add(estorno);
    await context.SaveChangesAsync();

    return Results.Created($"/api/internal/estornos/{estorno.Id}", estorno);
});

// Endpoint GET apenas para facilitar a visualização de que os dados foram gravados com sucesso
app.MapGet("/api/internal/estornos", async (AppDbContext context) =>
{
    var estornos = await context.Estornos.ToListAsync();
    return Results.Ok(estornos);
});

app.Run();

// Classe de request para o estorno
public record RequestEstorno(string IdTransacaoOriginal, decimal Valor, string Motivo);
