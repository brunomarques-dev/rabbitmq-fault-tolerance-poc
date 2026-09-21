using Estornos.Consumer.Worker;
using Estornos.Consumer.Worker.Data;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

// Configura o DbContext com SQL Server
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(connectionString), ServiceLifetime.Transient);

// Registra o Worker
builder.Services.AddHostedService<QueueConsumerWorker>();

var host = builder.Build();
host.Run();
