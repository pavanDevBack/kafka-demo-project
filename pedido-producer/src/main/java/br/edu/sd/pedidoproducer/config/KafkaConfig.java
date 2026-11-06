package br.edu.sd.pedidoproducer.config;

import br.edu.sd.pedidoproducer.model.ModoEnvio;
import br.edu.sd.pedidoproducer.model.Pedido;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.EnumMap;
import java.util.Map;


@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    public static final String TOPICO_PEDIDOS = "pedidos";
    public static final String TOPICO_ESTRITO = "pedidos-estrito";
    public static final String TOPICO_DLT = "pedidos.DLT";

    private final ProducerProperties props;

    @Bean
    public NewTopic topicoPedidos() {
        return TopicBuilder.name(TOPICO_PEDIDOS).partitions(3).replicas(3)
                .config("min.insync.replicas", "2").build();
    }

    @Bean
    public NewTopic topicoEstrito() {
        return TopicBuilder.name(TOPICO_ESTRITO).partitions(3).replicas(3)
                .config("min.insync.replicas", "3").build();
    }

    @Bean
    public NewTopic topicoDlt() {
        return TopicBuilder.name(TOPICO_DLT).partitions(3).replicas(3).build();
    }

    @Bean
    public ProdutoresPorModo produtoresPorModo() {
        var templates = new EnumMap<ModoEnvio, KafkaTemplate<String, Pedido>>(ModoEnvio.class);
        for (var modo : ModoEnvio.values()) {
            templates.put(modo, new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configuracao(modo))));
        }
        return new ProdutoresPorModo(templates);
    }

    private Map<String, Object> configuracao(ModoEnvio modo) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                "spring.json.add.type.headers", false, // o consumer tem a própria classe Pedido, em outro pacote
                ProducerConfig.ACKS_CONFIG, modo.getAcks(),
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, modo.isIdempotente(),
                // o cliente Kafka declara estes dois como int e recusa um Long
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) props.requestTimeout().toMillis(),
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) props.deliveryTimeout().toMillis(),
                ProducerConfig.MAX_BLOCK_MS_CONFIG, props.maxBlock().toMillis());
    }
}
