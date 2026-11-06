package br.edu.sd.pedidoproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PedidoProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PedidoProducerApplication.class, args);
    }
}
