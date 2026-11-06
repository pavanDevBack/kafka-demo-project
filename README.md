# kafka-demo — pedidos com Apache Kafka e Spring Boot

Exemplo prático do trabalho de Sistemas Distribuídos (Universidade La Salle).
Dois serviços que só conversam através de um cluster Kafka de 3 brokers:

| Serviço | Porta | O que faz |
| --- | --- | --- |
| `pedido-producer` | 8081 | Recebe pedidos por HTTP e publica no tópico `pedidos` |
| `pedido-consumer` | 8082 | Consome os pedidos, grava em `processados.log` e simula falhas |
| Kafka (3 brokers, KRaft) | 29092 · 39092 · 49092 | Cluster, sem ZooKeeper |
| Kafka UI | 8090 | Ver tópicos, partições, mensagens e grupos no navegador |

Stack: Java 21, Spring Boot 4, Spring for Apache Kafka 4, Lombok, Bean Validation, springdoc (Swagger UI).

## Pré-requisitos

- JDK 21
- Maven 3.9+ (ou a IDE: o IntelliJ já traz o Maven)
- Docker Desktop

## Como rodar

1. Subir o cluster:

   ```bash
   docker compose up -d
   docker compose ps          # os 4 containers devem estar "running"
   ```

2. Subir o **producer primeiro** (é ele que cria os tópicos com 3 partições e réplica 3):

   ```bash
   mvn -f pedido-producer spring-boot:run
   ```

3. Em outro terminal, subir o consumer:

   ```bash
   mvn -f pedido-consumer spring-boot:run
   ```

   No IntelliJ: abra a pasta `kafka-demo` (o `pom.xml` da raiz agrupa os dois módulos) e rode
   `PedidoProducerApplication` e depois `PedidoConsumerApplication`.

4. Testar: abra `requests.http` no IntelliJ e execute a requisição 1, ou use o Swagger UI em
   <http://localhost:8081/swagger-ui.html> e <http://localhost:8082/swagger-ui.html>.

Conferir se está tudo certo:

- log do producer: `[ACKS_ALL] pedido ... -> pedidos-X offset Y`
- log do consumer: `Recebido pedido ...` com a mesma partição e offset
- Kafka UI (<http://localhost:8090>): grupo `processador-pedidos` com as 3 partições atribuídas
- requisição 9: `processamentos: 1, duplicados: 0`

## Cenários da demonstração

Antes de cada cenário, zere o registro do consumer (requisição 10). Para ver o líder e o ISR de cada partição:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic pedidos
```

### 1. Broker líder cai: `acks=0` contra `acks=all`

1. Rode o `describe` e escolha um broker que seja líder de alguma partição (ex.: `kafka-2`).
2. Execute a requisição 4 (lote com `acks=0`) e, uns 3 segundos depois, `docker kill kafka-2`.
3. Compare a requisição 6 (producer) com a 9 (consumer): o producer acha que enviou tudo, mas faltam pedidos no consumer.
4. `docker start kafka-2`, espere uns 15 s, zere o registro e repita com a requisição 5 (`acks=all`): nenhum pedido se perde.

A perda com `acks=0` depende do momento exato da queda. Se der zero perdidos, repita com `intervaloMs=1`.

### 2. Réplicas insuficientes

1. `docker stop kafka-3`
2. Requisição 1 funciona (o tópico `pedidos` exige 2 réplicas).
3. Requisição 3 falha depois de ~15 s com `NOT_ENOUGH_REPLICAS` (o tópico `pedidos-estrito` exige 3) e o pedido vai para os pendentes (requisição 7).
4. `docker start kafka-3`, espere ~15 s e execute a requisição 8 para reenviar.

### 3. Kafka inteiro fora do ar

1. `docker compose stop kafka-1 kafka-2 kafka-3`
2. Requisição 1: demora ~5 s e o log mostra `FALHA ao enviar pedido`.
3. `docker compose start kafka-1 kafka-2 kafka-3`, espere ~20 s e reenvie os pendentes (requisição 8).

### 4. Consumer cai antes do commit (duplicata)

1. Execute a requisição 11 e depois a 1: o consumer registra o pedido e o processo morre.
2. Suba o consumer de novo: ele recebe **o mesmo pedido, no mesmo offset**. A requisição 9 mostra `duplicados: 1`.
3. Correção: zere o registro, repita 11 e 1 e suba o consumer com `--app.idempotencia=true`:

   ```bash
   mvn -f pedido-consumer spring-boot:run -Dspring-boot.run.arguments=--app.idempotencia=true
   ```

   O log mostra `Pedido ... já foi processado, ignorando a duplicata`.

### 5. Pedido inválido vai para a DLT

Requisição 2 (valor -10): o consumer tenta 3 vezes, com 1 s entre elas, e publica a mensagem em
`pedidos.DLT`. Confira no Kafka UI em Topics → pedidos.DLT → Messages.

### 6. Rebalanceamento

Suba uma segunda instância do consumer em outra porta:

```bash
mvn -f pedido-consumer spring-boot:run -Dspring-boot.run.arguments=--server.port=8083
```

Os logs das duas instâncias mostram `partitions assigned` com as partições divididas. Pare uma delas
e a outra volta a receber todas.

## Endpoints

| Serviço | Método e caminho | Para que serve |
| --- | --- | --- |
| producer | `POST /pedidos?modo=ACKS_ALL&topico=pedidos` | Envia um pedido (`{"cliente":"Ana","valor":50}`) |
| producer | `POST /pedidos/lote?quantidade=2000&intervaloMs=5&modo=ACKS_0` | Envia muitos pedidos em sequência |
| producer | `GET /pedidos/estatisticas` | Confirmados, falhas e pendentes |
| producer | `GET /pedidos/pendentes` | Pedidos que não chegaram ao Kafka |
| producer | `POST /pedidos/pendentes/reenviar` | Reenvia os pendentes com `acks=all` |
| consumer | `POST /simulacao/queda` | O próximo pedido é processado e o consumer cai antes do commit |
| consumer | `POST /simulacao/idempotencia?ativa=true` | Liga ou desliga a checagem de duplicata |
| consumer | `GET /processamentos/resumo` | Total processado, distintos e duplicados |
| consumer | `DELETE /processamentos` | Zera o registro |

## Problemas comuns

| Sintoma | Solução |
| --- | --- |
| `Connection to node -1 ... could not be established` | O cluster não está de pé: `docker compose ps` e `docker logs kafka-1`. |
| `cannot find symbol` em `log`, `builder()` ou `getAcks()` | Lombok sem processar anotações. No IntelliJ: Settings → Build → Compiler → Annotation Processors → Enable, depois Rebuild. |
| Erro vindo de `org.springdoc` na subida | Versão do springdoc incompatível com a do Spring Boot. Ajuste `springdoc.version` no `pom.xml`. |
| Erro de conversão do JSON no consumer | `spring.json.value.default.type` precisa ser o pacote exato da classe `Pedido` do consumer. |
| Tudo lento ou containers caindo | Reserve pelo menos 4 GB de memória para o Docker. |
| Recomeçar do zero | `docker compose down` apaga containers e mensagens; suba de novo e rode o producer primeiro. |
