# Handoff para o Claude

Este arquivo descreve o estado do trabalho realizado em 2026-09-06 no fork do
Kine com backend MongoDB. Leia-o antes de alterar ou executar o projeto.

## Regras importantes

- Preserve alterações do usuário que não façam parte da tarefa atual.
- `spikes/mongo.py` já estava modificado antes deste trabalho. A mudança remove
  aspas da `MONGO_URI` lida do arquivo de ambiente e não foi feita nem alterada
  durante esta tarefa.
- Nunca imprima ou grave no repositório o conteúdo de `MONGO_URI`.
- A credencial usada nos testes fica em
  `/home/vinny/.config/kine-mongo/env`.
- O worktree não foi commitado.

## Objetivo da sessão

1. Criar instruções para agentes sempre consultarem este arquivo.
2. Revisar o projeto em busca de bugs.
3. Subir um K3s para testar o Kine.

Foi criado `AGENTS.md` na raiz. Ele obriga qualquer agente a localizar e ler
`CLAUDE.md` integralmente antes de inspecionar, editar, testar ou executar o
repositório.

## Bugs encontrados e alterações feitas

### 1. Expiração MongoDB removia chaves sem tombstone

Problema original:

- `Record` possuía `expires_at`.
- `setup` criava o índice TTL `lease_ttl`.
- O MongoDB apagava diretamente o documento quando o lease expirava.
- A remoção direta não gerava uma tombstone do protocolo etcd.
- Dependendo dos leases de revisões sucessivas, apagar a revisão mais nova
  também podia revelar uma revisão histórica anterior e ressuscitar um valor.
- O change stream ignorava deletes físicos, portanto watchers não recebiam o
  evento de expiração esperado.

Correção aplicada:

- Removido `Record.ExpiresAt`.
- Removido `expiryFor` e seu uso em `Create`/`Update`.
- Removida a criação do índice TTL.
- `Backend.Start` agora inicia `go ttl.Run(ctx, b)`, reutilizando o mecanismo
  comum de leases do projeto. Ele executa `Delete` com comparação de revisão e
  produz a tombstone correta.
- `setup` remove o índice legado `lease_ttl` em bancos existentes. O erro
  MongoDB `IndexNotFound` (código 27) é ignorado; outros erros continuam fatais.

Arquivos envolvidos:

- `pkg/drivers/mongo/schema.go`
- `pkg/drivers/mongo/mongo.go`
- `pkg/drivers/mongo/crud.go`

Cobertura adicionada:

- `TestLeaseExpiraComTombstone`, em
  `pkg/drivers/mongo/integration_test.go`.

### 2. Escritas concorrentes podiam perder eventos de watch

Problema original:

- Cada registro Mongo é persistido em duas operações: `InsertOne`, que fornece
  o `clusterTime` usado como revisão, e `UpdateByID`, que grava essa revisão.
- Duas chamadas concorrentes podiam executar os inserts na ordem A/B, mas os
  updates na ordem B/A.
- O change stream recebia primeiro a revisão maior de B e avançava seu corte.
- Quando a revisão menor de A chegava, `watchLoop` a descartava como já
  entregue (`r.Rev <= corte`).
- No primeiro teste K3s com MongoDB, isso deixou o apiserver preso em
  `autoregister-completion`: alguns objetos eram persistidos, mas seus eventos
  não chegavam aos caches.

Correção aplicada:

- Adicionados `insertMu` e `revisionMu` ao backend.
- `append` agora usa um pipeline ordenado:
  1. o insert é feito sob `insertMu`;
  2. a fase de update é reservada em `revisionMu` antes de liberar o próximo
     insert;
  3. inserts e updates adjacentes ainda podem se sobrepor, mas updates chegam
     ao oplog na mesma ordem das revisões dos inserts.
- Uma primeira implementação serializava as duas operações inteiras. Ela
  preservava a ordem, mas criou filas ainda maiores e foi substituída pelo
  pipeline atual.

Cobertura adicionada:

- `TestWatchComEscritasConcorrentes`, em
  `pkg/drivers/mongo/integration_test.go`, cria 16 chaves simultaneamente e
  exige que todos os eventos cheguem em ordem estritamente crescente.

Limitação conhecida:

- Os mutexes coordenam escritas dentro de uma única instância do Kine. A
  ordenação entre múltiplos processos Kine escrevendo diretamente no mesmo
  MongoDB ainda precisa de desenho e validação específicos.

## Validações executadas

Todos estes comandos passaram depois das alterações finais:

```bash
GOCACHE=/tmp/kine-go-cache go test ./...
GOCACHE=/tmp/kine-go-cache go vet ./...
git diff --check
```

Também passaram os testes de integração reais contra o MongoDB configurado:

```bash
set -a
source /home/vinny/.config/kine-mongo/env
set +a
KINE_MONGO_TEST_URI="$MONGO_URI" \
  GOCACHE=/tmp/kine-go-cache \
  go test -tags=integration ./pkg/drivers/mongo ./test/mongo
```

Isso inclui testes diretos do backend e testes pelo protocolo etcd v3.

## Testes K3s realizados

### Imagens e rede

- Foi construída a imagem local `kine-mongo:codex-test` a partir deste
  checkout, usando o target `package` do `Dockerfile`.
- Foi baixada `rancher/k3s:latest`, que no momento do teste correspondia a
  K3s `v1.34.1+k3s1`.
- Foi criada a rede Docker `kine-k3s-test-net`.

### Tentativas com MongoDB Atlas

O Kine e o K3s foram colocados na mesma rede Docker porque, no Docker Desktop,
`host.docker.internal` não conseguia alcançar o processo Kine escutando no WSL.

Resultados:

- O apiserver conseguiu usar o Kine/MongoDB e chegou a ficar `ready`.
- O nó chegou a ser registrado.
- A correção de ordenação eliminou o travamento inicial em
  `autoregister-completion`.
- Entretanto, durante a carga inicial, a latência e a fila de escritas no
  Atlas ultrapassaram timeouts de 5 a 10 segundos usados pelas eleições de
  líder e atualizações do kubelet.
- O scheduler/controller-manager perdeu a eleição e o processo K3s encerrou.
- Desabilitar o cloud-controller evitou uma falha específica de RBAC desse
  componente, mas não resolveu os timeouts de escrita sob carga.

Conclusão atual: os testes de integração do Mongo passam, mas o backend MongoDB
ainda não sustenta de forma confiável a carga de bootstrap de um K3s real com
os timeouts padrão. Não considere MT-3 concluído.

Bancos temporários criados no Atlas pelas tentativas de K3s:

- `k3s_codex_test`
- `k3s_codex_test_v2`
- `k3s_codex_test_v3`
- `k3s_codex_test_v4`

Eles não foram removidos automaticamente. Confirme o conteúdo e obtenha
autorização apropriada antes de apagá-los. As coleções criadas pelos testes Go
de integração são removidas pelo próprio cleanup dos testes.

### Smoke test ativo com SQLite

Para separar problemas do driver MongoDB dos problemas do caminho
K3s -> Kine, foi iniciado um Kine deste checkout com SQLite e um K3s somente de
plano de controle (`--disable-agent`).

Estado confirmado ao criar este documento:

- container `kine-test`: ativo, imagem `kine-mongo:codex-test`;
- container `kine-k3s-test`: ativo, imagem `rancher/k3s:latest`;
- `kubectl get --raw=/readyz`: `ok`;
- namespace `teste-kine`: criado;
- deployment `teste-kine`: criado, escalado de 2 para 4 réplicas e atualizado
  de `rancher/mirrored-pause:3.9` para `3.6`;
- ReplicaSets e Pods foram criados pelo controller-manager.

Os Pods ficam `Pending` porque não há agente/nó. O agente foi desabilitado pois
o kubelet/containerd aninhado em container privilegiado no Docker Desktop/WSL
não ficou operacional. A tentativa com `--docker` também falhou porque o
cri-dockerd esperava `/var/lib/docker` dentro do container.

Comandos úteis para inspeção:

```bash
docker ps -a --filter name=kine-
docker exec kine-k3s-test kubectl get --raw=/readyz
docker exec kine-k3s-test kubectl get deployment,replicaset,pods \
  -n teste-kine -o wide
docker logs kine-k3s-test
docker logs kine-test
```

O ambiente foi deixado ativo intencionalmente para inspeção. Não remova os
containers, a rede ou as imagens sem confirmar que o teste não é mais
necessário.

## Estado esperado do worktree

Arquivos criados nesta sessão:

- `AGENTS.md`
- `CLAUDE.md`

Arquivos modificados nesta sessão:

- `pkg/drivers/mongo/crud.go`
- `pkg/drivers/mongo/integration_test.go`
- `pkg/drivers/mongo/mongo.go`
- `pkg/drivers/mongo/schema.go`

Alteração anterior do usuário, preservada:

- `spikes/mongo.py`

## Próximos passos recomendados

1. Medir separadamente latência, throughput e tamanho máximo da fila de
   `append` durante o bootstrap K3s.
2. Decidir como preservar a ordem de revisões sem limitar o throughput de
   escrita — por exemplo, batching/coordenação no Mongo — mantendo em mente a
   ordenação entre múltiplas instâncias do Kine.
3. Adicionar um teste de carga que simule as escritas concorrentes e as
   renovações de lease do scheduler/controller-manager com deadlines curtos.
4. Repetir o MT-3 em uma VM Linux real, com K3s instalado normalmente, para
   remover as limitações de kubelet aninhado do Docker Desktop/WSL.
5. Só considerar MT-3 aprovado quando o nó ficar `Ready`, pods do sistema
   estabilizarem e um deployment concluir scale e rolling update usando o
   backend MongoDB.
