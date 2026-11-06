package br.edu.sd.pedidoproducer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("app.producer")
public record ProducerProperties(
        String bootstrapServers,
        Duration requestTimeout,
        Duration deliveryTimeout,
        Duration maxBlock) {
}
