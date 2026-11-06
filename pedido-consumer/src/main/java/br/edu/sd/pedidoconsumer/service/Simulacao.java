package br.edu.sd.pedidoconsumer.service;

import br.edu.sd.pedidoconsumer.config.ConsumerProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class Simulacao {

    private final AtomicBoolean quedaNoProximo = new AtomicBoolean(false);
    private final AtomicBoolean idempotencia;

    public Simulacao(ConsumerProperties props) {
        this.idempotencia = new AtomicBoolean(props.idempotencia());
    }

    public void agendarQueda() {
        quedaNoProximo.set(true);
    }

    public boolean deveCair() {
        return quedaNoProximo.getAndSet(false);
    }

    public boolean idempotenciaAtiva() {
        return idempotencia.get();
    }

    public void definirIdempotencia(boolean ativa) {
        idempotencia.set(ativa);
    }
}
