package br.edu.sd.pedidoconsumer.listener;

import br.edu.sd.pedidoconsumer.model.Pedido;
import br.edu.sd.pedidoconsumer.service.RegistroProcessamento;
import br.edu.sd.pedidoconsumer.service.Simulacao;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PedidoListener {

    private final RegistroProcessamento registro;
    private final Simulacao simulacao;

    @KafkaListener(topics = {"pedidos", "pedidos-estrito"})
    public void consumir(ConsumerRecord<String, Pedido> record, Acknowledgment ack) {
        var pedido = record.value();
        log.info("Recebido pedido {} | {}-{} offset {}", pedido.id(), record.topic(),
                record.partition(), record.offset());

        if (simulacao.idempotenciaAtiva() && registro.jaProcessado(pedido.id())) {
            log.warn("Pedido {} já foi processado, ignorando a duplicata", pedido.id());
            ack.acknowledge();
            return;
        }

        if (pedido.valor() <= 0) {
            throw new IllegalArgumentException("valor inválido: " + pedido.valor());
        }

        registro.registrar(pedido.id(), record.partition(), record.offset());

        if (simulacao.deveCair()) {
            log.error(">>> SIMULANDO QUEDA: pedido {} processado, offset NÃO confirmado", pedido.id());
            // halt mata o processo na hora, como uma queda de energia; System.exit desligaria com calma
            Runtime.getRuntime().halt(1);
        }

        ack.acknowledge();
    }
}
