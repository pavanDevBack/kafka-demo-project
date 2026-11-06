package br.edu.sd.pedidoconsumer.model;

public record Pedido(String id, String cliente, double valor, long criadoEm) {
}
