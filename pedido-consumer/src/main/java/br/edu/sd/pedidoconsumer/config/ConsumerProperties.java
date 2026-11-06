package br.edu.sd.pedidoconsumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app")
public record ConsumerProperties(boolean idempotencia, String arquivoRegistro) {
}
