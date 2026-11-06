package br.edu.sd.pedidoproducer.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;


public record NovoPedidoRequest(
        @Schema(example = "Ana") @NotBlank String cliente,
        @Schema(example = "50") @NotNull Double valor) {
}
