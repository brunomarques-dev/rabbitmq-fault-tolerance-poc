using Estornos.Processor.Api.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace Estornos.Processor.Api.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {
    }

    public DbSet<Estorno> Estornos { get; set; } = null!;
}
