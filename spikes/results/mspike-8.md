# MSPIKE-8 — Janela do oplog e invalidação real de Change Stream

**Status:** ✅ concluído · **Data:** 2026-09-07

Fecha a lacuna que o MW-3 deixou: a recuperação estava implementada e testada no *mecanismo*, mas nunca no *gatilho*. Aqui a invalidação é provocada de verdade.

## A janela do oplog no M0

O `collStats` do oplog é bloqueado no free tier, mas `local.oplog.rs` é **legível**. Medindo o primeiro e o último registro:

```
mais antigo:  2026-09-06 17:33:52
mais recente: 2026-09-06 21:58:02
janela:       15.850 s = 4,4 h
entradas:     ~16.494
```

**~4,4 horas de janela.** Um watch pode ficar até esse tempo para trás antes de invalidar. A janela é por volume, não por tempo: quanto mais escrita, mais curta.

## A invalidação acontece, e com o código esperado

Abrindo change stream com `startAtOperationTime` progressivamente mais antigo:

| Ponto de retomada | Resultado |
|---|---|
| 1 hora atrás | aceito |
| 1 dia atrás | aceito |
| **30 dias atrás** | **recusado — código 286** |

```
(ChangeStreamHistoryLost) PlanExecutor error during aggregation ::
caused by :: Resume of change stream was not possible, as the resume
point may no longer be in the oplog.
```

**286 = `ChangeStreamHistoryLost`**, que é exatamente um dos dois códigos que o `historicoPerdido()` do driver trata (o outro é 280, `ChangeStreamFatalError`).

Curiosidade útil: aceitar "1 dia atrás" quando a janela medida é de 4,4 h mostra que o MongoDB não valida o timestamp na abertura — o erro só aparece quando o stream tenta ler. Por isso o driver classifica o erro em `cs.Err()` e não no `Watch()`.

## Validação end-to-end

`TestInvalidacaoRealDoOplog` (em `pkg/drivers/mongo/integration_test.go`):

1. Provoca a invalidação real com um ponto de retomada de 30 dias atrás.
2. Confirma que `historicoPerdido()` reconhece o erro — se não reconhecesse, a recuperação do MW-3 nunca dispararia.
3. Confirma que `abrirStream(nil)` consegue abrir um stream novo depois da falha.

O teste tem um `Skip` para o caso de o MongoDB passar a aceitar 30 dias: melhor pular declaradamente do que passar sem ter observado nada.

## Conclusão

O risco está tratado. O que muda na operação:

- A janela de **4,4 h** é folgada para um kine saudável, mas some se o processo ficar parado uma tarde ou se o volume de escrita subir muito.
- Quando invalida, o driver detecta e preenche o buraco lendo a coleção entre a última revisão observada e agora — sem perder evento.
- A janela **não é observável em runtime** no M0 (`collStats` bloqueado), então não há como alertar preventivamente. O desenho certo é o que está: tratar a invalidação quando ela ocorre, em vez de tentar prevê-la.

## Reproduzir

```bash
spikes/mspike-8-oplog.py
go test -tags=integration ./pkg/drivers/mongo/ -run TestInvalidacaoRealDoOplog -v
```
