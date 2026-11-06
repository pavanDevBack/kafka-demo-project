package br.edu.sd.pedidoproducer.controller;

import br.edu.sd.pedidoproducer.config.KafkaConfig;
import br.edu.sd.pedidoproducer.dto.NovoPedidoRequest;
import br.edu.sd.pedidoproducer.model.ModoEnvio;
import br.edu.sd.pedidoproducer.model.Pedido;
import br.edu.sd.pedidoproducer.service.PedidoService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Tag(name = "Pedidos", description = "Envio de pedidos para o Kafka")
@RestController
@RequestMapping("/pedidos")
@RequiredArgsConstructor
public class PedidoController {

    private final PedidoService service;

    @Operation(summary = "Envia um pedido escolhendo o modo de acks e o tópico")
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED) // a confirmação do Kafka chega depois, no callback
    public Map<String, Object> criar(@Valid @RequestBody NovoPedidoRequest novo,
                                     @RequestParam(defaultValue = "ACKS_ALL") ModoEnvio modo,
                                     @RequestParam(defaultValue = KafkaConfig.TOPICO_PEDIDOS) String topico) {
        var pedido = Pedido.builder()
                .id(UUID.randomUUID().toString())
                .cliente(novo.cliente())
                .valor(novo.valor())
                .criadoEm(System.currentTimeMillis())
                .build();
        service.enviar(pedido, modo, topico);
        return Map.of("pedidoId", pedido.id(), "modo", modo, "topico", topico);
    }

    @Operation(summary = "Envia muitos pedidos em sequência, numa virtual thread")
    @PostMapping("/lote")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public String lote(@RequestParam int quantidade,
                       @RequestParam(defaultValue = "10") long intervaloMs,
                       @RequestParam(defaultValue = "ACKS_ALL") ModoEnvio modo) {
        service.enviarLote(quantidade, intervaloMs, modo);
        return "Lote de %d pedidos iniciado com %s".formatted(quantidade, modo);
    }

    @Operation(summary = "Confirmados, falhas e pendentes desde o último lote")
    @GetMapping("/estatisticas")
    public Map<String, Object> estatisticas() {
        return service.estatisticas();
    }

    @Operation(summary = "Pedidos que não chegaram ao Kafka")
    @GetMapping("/pendentes")
    public List<PedidoService.PedidoPendente> pendentes() {
        return service.listarPendentes();
    }

    @Operation(summary = "Reenvia os pendentes com acks=all")
    @PostMapping("/pendentes/reenviar")
    public Map<String, Object> reenviar() {
        return Map.of("reenviados", service.reenviarPendentes());
    }
}
