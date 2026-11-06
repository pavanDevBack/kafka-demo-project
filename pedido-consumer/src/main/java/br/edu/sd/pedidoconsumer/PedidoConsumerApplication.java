package br.edu.sd.pedidoconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PedidoConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PedidoConsumerApplication.class, args);
    }
}
