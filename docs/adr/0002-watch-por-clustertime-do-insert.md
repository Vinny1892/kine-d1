# ADR-0002 — O watch escuta só `insert` e deriva a revisão do `clusterTime` do evento

**Status:** aceito · **Data:** 2026-09-07 · **Evidência:** [mt3.md](../../spikes/results/mt3.md)

## Contexto

A revisão do etcd vem do `clusterTime` do MongoDB ([ADR-0001](0001-revisao-por-clustertime.md)). Esse valor só é conhecido **depois** do insert, via `session.OperationTime()`. Por isso o `append` grava em duas operações: o `InsertOne` e um `UpdateByID` que persiste o campo `rev`.

A primeira implementação do watch descartava o evento do insert (`if r.Rev == 0 { continue }`) e esperava o evento do **update**, que já traz o campo preenchido.

## O problema

Sob concorrência os inserts saem na ordem A/B, mas os updates podem sair B/A — são duas viagens de rede independentes. O watch avançava sua revisão de corte com o valor maior (B) e, quando o menor (A) chegava, descartava como já entregue. **Eventos desapareciam.**

Isso não é teórico: travou o bootstrap de um k3s real em `autoregister-completion`. Alguns objetos eram persistidos e seus eventos nunca chegavam aos caches do apiserver.

## A alternativa que foi descartada

Serializar as escritas com dois mutexes (`insertMu`, `revisionMu`), formando um pipeline onde os updates alcançam o oplog na mesma ordem dos inserts.

Funcionava e preservava a ordem, mas **criava fila**. Sob a carga de bootstrap do k3s a espera ultrapassava os deadlines de 5-10 s da leader election, e o scheduler e o controller-manager perdiam a eleição — derrubando o cluster. A correção do primeiro bug produzia o segundo.

## Decisão

**O watch escuta apenas `operationType: "insert"` e deriva a revisão do `clusterTime` do próprio evento**, em vez de ler o campo `rev` do documento.

```go
pipeline := mongo.Pipeline{
    {{Key: "$match", Value: bson.M{"operationType": "insert"}}},
}
// ...
rev, err := EncodeRevision(ev.ClusterTime, b.cfg.EpochBase)
```

## Justificativa

**O evento de insert já carrega a revisão.** O `clusterTime` do insert é exatamente o valor que a escrita grava em `rev` — medido no MSPIKE-4: `Timestamp(1788730614, 7)` idêntico nos dois lados. Esperar o update para obter um número que já está em mãos era o erro.

**Escutar só inserts é correto porque o kine é um log append-only.** Toda mutação — criar, atualizar, apagar — insere um documento novo. O único update que existe é o que grava `rev`, e ele não representa mutação alguma. Filtrar updates não perde informação; remove ruído.

**A ordem passa a ser propriedade da escolha, não resultado de esforço.** Como a revisão *é* o clusterTime, a ordem em que o oplog entrega os eventos é a ordem das revisões. Não há nada a reordenar, nada a serializar, nada a coordenar.

**Ganho colateral que resolveu um problema aberto:** o oplog é global ao cluster MongoDB. A limitação registrada na abordagem com mutexes — "coordenam apenas dentro de uma instância do kine" — deixa de existir. Validado em `TestDuasInstancias`: dois backends sobre a mesma coleção produzem revisões globalmente monotônicas e distintas, e o watch de um vê as escritas do outro em ordem.

**O ganho de performance foi grande e mensurável.** Sem a fila:

| | Com mutexes | Escutando inserts |
|---|---|---|
| `readyz` do apiserver | ~95 s | **30 s** |
| 50 configmaps de carga | 77 s | **14 s** |

## Consequências

- O `UpdateByID` sai do caminho crítico da ordenação. Ele passa a servir apenas às consultas históricas (`After`, `List`, `Get` por revisão), que leem depois do fato.
- Se o processo morrer entre o insert e o update, o documento fica com `rev = 0`: invisível para `After` e `List`, que filtram por `rev > 0`. O evento **já foi entregue** ao watch, então há uma janela em que um watcher viu algo que uma recuperação histórica não veria. O órfão é inerte — não corrompe estado, e a chave pode ser reescrita porque a constraint `(name, prev_revision)` continua valendo.
- **Não "conserte" isso reintroduzindo a espera pelo update.** É exatamente o bug que travava o apiserver. Se a janela do órfão precisar ser fechada, o caminho é reconciliar `rev = 0` no start, não serializar a escrita.
- O `recordsToEvents` recebe a revisão já preenchida pelo watch, não do documento.
