package br.edu.sd.pedidoproducer.config;

import br.edu.sd.pedidoproducer.model.ModoEnvio;
import br.edu.sd.pedidoproducer.model.Pedido;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

public record ProdutoresPorModo(Map<ModoEnvio, KafkaTemplate<String, Pedido>> templates) {

    public KafkaTemplate<String, Pedido> de(ModoEnvio modo) {
        return templates.get(modo);
    }
}
