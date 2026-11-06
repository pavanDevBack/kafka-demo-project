package br.edu.sd.pedidoproducer.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ModoEnvio {

    ACKS_0("0", false),
    ACKS_1("1", false),
    ACKS_ALL("all", true);

    private final String acks;
    private final boolean idempotente;
}
