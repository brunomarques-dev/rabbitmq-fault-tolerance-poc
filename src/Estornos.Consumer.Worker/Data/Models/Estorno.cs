using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Estornos.Consumer.Worker.Data.Models;

[Table("Estornos")]
public class Estorno
{
    [Key]
    public Guid Id { get; set; }

    [Required]
    [MaxLength(100)]
    public string IdTransacaoOriginal { get; set; } = string.Empty;

    [Required]
    [Column(TypeName = "decimal(18,2)")]
    public decimal Valor { get; set; }

    [MaxLength(500)]
    public string Motivo { get; set; } = string.Empty;

    [Required]
    [MaxLength(50)]
    public string Status { get; set; } = "Pendente";

    public DateTime DataCriacao { get; set; } = DateTime.UtcNow;
}
