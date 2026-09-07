# Spikes

Medições contra um **MongoDB Atlas real**, para validar premissas antes de
escrever código de produção. Foi essa disciplina que pegou o modelo de custo
do D1 errado por 70x, uma mitigação de transação silenciosamente errada, e o
bug de ordenação que travava o apiserver.

## Rodando

Credenciais em `~/.config/kine-mongo/env` (fora do repositório):

```bash
MONGO_URI='mongodb+srv://usuario:senha@cluster.exemplo.mongodb.net/?retryWrites=true&w=majority'
```

As aspas simples importam: a URI contém `&`, que o shell interpretaria como
operador ao dar `source`.

```bash
python3 -m venv ~/.config/kine-mongo/venv
~/.config/kine-mongo/venv/bin/pip install pymongo
~/.config/kine-mongo/venv/bin/python spikes/mspike-1-setup.py
```

## Estado

| Spike | O que responde | Status |
|---|---|---|
| MSPIKE-1 | Cluster M0: identidade, limites, linha de base | ✅ [concluído](results/mspike-1.md) |
| MSPIKE-2 | Change Streams funcionam no M0? | ✅ [concluído](results/mspike-2-3.md) |
| MSPIKE-3 | Transações multi-documento funcionam no M0? | ✅ [concluído](results/mspike-2-3.md) |
| MSPIKE-4 | Como gerar revisões: contador vs clusterTime | ✅ [concluído](results/mspike-4.md) |
| MSPIKE-5 | Latência das queries reais do kine | ✅ [concluído](results/mspike-5-9.md) |
| MSPIKE-6 | Teto de 100 ops/s: o que acontece ao estourar? | ⬜ aberto |
| MSPIKE-7 | Tamanho em disco de um cluster | ✅ [concluído](results/mspike-5-9.md) |
| MSPIKE-8 | Janela do oplog e invalidação de Change Stream | ✅ [concluído](results/mspike-8.md) |
| MSPIKE-9 | Modelo de documento e índices | ✅ [concluído](results/mspike-5-9.md) |
| MT-3 | k3s real com nó Ready | ✅ [concluído](results/mt3.md) |

O trabalho descartado com o Cloudflare D1 está em
[docs/d1-descartado/](../docs/d1-descartado/README.md).
