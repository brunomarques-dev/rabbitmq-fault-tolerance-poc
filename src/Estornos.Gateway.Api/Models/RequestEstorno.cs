namespace Estornos.Gateway.Api.Models;

public record RequestEstorno(string IdTransacaoOriginal, decimal Valor, string Motivo);
