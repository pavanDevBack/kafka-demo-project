package br.edu.sd.pedidoconsumer.controller;

import br.edu.sd.pedidoconsumer.service.RegistroProcessamento;
import br.edu.sd.pedidoconsumer.service.Simulacao;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Tag(name = "Simulação", description = "Controle dos cenários de falha do consumer")
@RestController
@RequiredArgsConstructor
public class SimulacaoController {

    private final Simulacao simulacao;
    private final RegistroProcessamento registro;

    @Operation(summary = "O consumer cai logo depois de processar o próximo pedido, antes do commit")
    @PostMapping("/simulacao/queda")
    public String agendarQueda() {
        simulacao.agendarQueda();
        return "O consumer vai cair depois de processar o próximo pedido";
    }

    @Operation(summary = "Liga ou desliga a checagem de duplicata")
    @PostMapping("/simulacao/idempotencia")
    public String idempotencia(@RequestParam boolean ativa) {
        simulacao.definirIdempotencia(ativa);
        return "Idempotência " + (ativa ? "ligada" : "desligada");
    }

    @Operation(summary = "Total processado, pedidos distintos e duplicados")
    @GetMapping("/processamentos/resumo")
    public Map<String, Object> resumo() {
        return registro.resumo();
    }

    @Operation(summary = "Zera o registro antes de um cenário")
    @DeleteMapping("/processamentos")
    public String limpar() {
        registro.limpar();
        return "Registro limpo";
    }
}
