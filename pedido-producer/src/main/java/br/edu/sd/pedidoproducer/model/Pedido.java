package br.edu.sd.pedidoproducer.model;

import lombok.Builder;

@Builder
public record Pedido(String id, String cliente, double valor, long criadoEm) {
}
