package br.edu.sd.pedidoconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaOperations<?, ?> template) {
        // por padrão publica em <tópico>.DLT, na mesma partição
        var recoverer = new DeadLetterPublishingRecoverer(template);
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        handler.setCommitRecovered(true);
        handler.setRetryListeners((record, ex, tentativa) ->
                log.warn("Tentativa {} falhou no offset {}: {}", tentativa, record.offset(), ex.getMessage()));
        return handler;
    }
}
