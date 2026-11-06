package br.edu.sd.pedidoconsumer.service;

import br.edu.sd.pedidoconsumer.config.ConsumerProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RegistroProcessamento {

    private final Path arquivo;
    private final Set<String> idsProcessados = ConcurrentHashMap.newKeySet();

    public RegistroProcessamento(ConsumerProperties props) {
        this.arquivo = Path.of(props.arquivoRegistro());
        lerLinhas().forEach(linha -> idsProcessados.add(idDa(linha)));
    }

    public boolean jaProcessado(String pedidoId) {
        return idsProcessados.contains(pedidoId);
    }

    public synchronized void registrar(String pedidoId, int particao, long offset) {
        var linha = "%s;%d;%d;%s%n".formatted(pedidoId, particao, offset, Instant.now());
        try {
            Files.writeString(arquivo, linha, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        idsProcessados.add(pedidoId);
    }

    public synchronized Map<String, Object> resumo() {
        var linhas = lerLinhas();
        long distintos = linhas.stream().map(RegistroProcessamento::idDa).distinct().count();
        return Map.of("processamentos", linhas.size(), "pedidosDistintos", distintos,
                "duplicados", linhas.size() - distintos);
    }

    public synchronized void limpar() {
        try {
            Files.deleteIfExists(arquivo);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        idsProcessados.clear();
    }

    private List<String> lerLinhas() {
        try {
            return Files.exists(arquivo)
                    ? Files.readAllLines(arquivo).stream().filter(l -> !l.isBlank()).toList()
                    : List.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String idDa(String linha) {
        return linha.split(";")[0];
    }
}
