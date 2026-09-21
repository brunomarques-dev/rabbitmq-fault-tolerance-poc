using Estornos.Consumer.Worker.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace Estornos.Consumer.Worker.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {
    }

    public DbSet<Estorno> Estornos { get; set; } = null!;
}
