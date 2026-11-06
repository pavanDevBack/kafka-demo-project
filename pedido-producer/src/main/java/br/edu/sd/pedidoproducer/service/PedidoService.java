package br.edu.sd.pedidoproducer.service;

import br.edu.sd.pedidoproducer.config.KafkaConfig;
import br.edu.sd.pedidoproducer.config.ProdutoresPorModo;
import br.edu.sd.pedidoproducer.model.ModoEnvio;
import br.edu.sd.pedidoproducer.model.Pedido;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoService {

    public record PedidoPendente(Pedido pedido, String topico) {
    }

    private final ProdutoresPorModo produtores;
    private final Queue<PedidoPendente> pendentes = new ConcurrentLinkedQueue<>();
    private final AtomicLong confirmados = new AtomicLong();
    private final AtomicLong falhas = new AtomicLong();

    public void enviar(Pedido pedido, ModoEnvio modo, String topico) {
        try {
            // a chave é o id do pedido: todos os eventos do mesmo pedido vão para a mesma partição
            produtores.de(modo).send(topico, pedido.id(), pedido)
                    .whenComplete((resultado, erro) -> {
                        if (erro != null) {
                            registrarFalha(pedido, topico, erro);
                            return;
                        }
                        confirmados.incrementAndGet();
                        var meta = resultado.getRecordMetadata();
                        // com acks=0 o offset vem -1: o broker nunca respondeu
                        log.info("[{}] pedido {} -> {}-{} offset {}", modo, pedido.id(),
                                meta.topic(), meta.partition(), meta.offset());
                    });
        } catch (Exception erro) {
            // sem metadados do cluster, o send() pode falhar antes de devolver o future
            registrarFalha(pedido, topico, erro);
        }
    }

    private void registrarFalha(Pedido pedido, String topico, Throwable erro) {
        falhas.incrementAndGet();
        pendentes.add(new PedidoPendente(pedido, topico));
        log.error("FALHA ao enviar pedido {}: {}", pedido.id(), erro.getMessage());
    }

    public void enviarLote(int quantidade, long intervaloMs, ModoEnvio modo) {
        confirmados.set(0);
        falhas.set(0);
        var prefixo = "lote" + System.currentTimeMillis() % 100_000;

        Thread.ofVirtual().name(prefixo).start(() -> {
            for (int i = 1; i <= quantidade; i++) {
                var pedido = Pedido.builder()
                        .id(prefixo + "-" + i)
                        .cliente("cliente-" + i % 10)
                        .valor(100.0)
                        .criadoEm(System.currentTimeMillis())
                        .build();
                enviar(pedido, modo, KafkaConfig.TOPICO_PEDIDOS);
                dormir(intervaloMs);
            }
            log.info("Lote {} finalizado: {} pedidos enviados com {}", prefixo, quantidade, modo);
        });
    }

    public int reenviarPendentes() {
        // esvazia a fila antes de reenviar, para uma nova falha não entrar em loop
        var lista = Stream.generate(pendentes::poll).takeWhile(Objects::nonNull).toList();
        lista.forEach(item -> enviar(item.pedido(), ModoEnvio.ACKS_ALL, item.topico()));
        return lista.size();
    }

    public List<PedidoPendente> listarPendentes() {
        return List.copyOf(pendentes);
    }

    public Map<String, Object> estatisticas() {
        return Map.of("confirmados", confirmados.get(), "falhas", falhas.get(),
                "pendentes", pendentes.size());
    }

    private static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
