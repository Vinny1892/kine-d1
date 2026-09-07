package mongo

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// Métricas do backend MongoDB.
//
// O alvo aqui é deliberado, e vem do que o MSPIKE-6 mediu: ao estourar o teto
// de operações do M0, o Atlas NÃO devolve erro — ele enfileira. Zero erros até
// 6x o teto, mas o p99 de escrita sai de 810 ms para 63 s.
//
// Ou seja: monitorar taxa de erro não detecta o problema mais provável. O que
// detecta é latência de escrita e atraso do change stream — com o RenewDeadline
// de 10 s da leader election, um p99 acima de ~1 s significa cluster a caminho
// de perder a liderança, sem nenhum erro no log para explicar.
var (
	OpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_mongo_ops_total",
		Help: "Total de operações no MongoDB por tipo e resultado",
	}, []string{"op", "result"})

	// Buckets alinhados com o que importa: 1 ms a ~16 s. O RenewDeadline de
	// 10 s da leader election cai dentro da faixa, então dá para alertar antes.
	OpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kine_mongo_op_duration_seconds",
		Help:    "Duração das operações no MongoDB",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"op"})

	// A métrica mais importante deste conjunto. Um watch atrasado é um cluster
	// que não vê suas próprias mudanças: reconciliação para e informers ficam
	// obsoletos, sem erro algum. Medido em 44 s a 6x o teto (MSPIKE-6).
	ChangeStreamLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_mongo_change_stream_lag_seconds",
		Help: "Idade do último evento recebido do change stream",
	})

	ChangeStreamReconnects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_mongo_change_stream_reconnects_total",
		Help: "Reconexões do change stream, por motivo",
	}, []string{"motivo"})

	// O M0 não expande: ao encher, o cluster para. Um cluster k3s ocupa ~1 MB
	// (MSPIKE-7), então a folga é grande — mas a falha é total.
	StorageBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kine_mongo_storage_bytes",
		Help: "Bytes ocupados no MongoDB, por componente",
	}, []string{"componente"})

	CurrentRevisionGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_mongo_current_revision",
		Help: "Revisão atual conhecida pelo backend",
	})

	CompactedRevisionGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_mongo_compacted_revision",
		Help: "Revisão até a qual o backend já compactou",
	})
)

var colecionadores = []prometheus.Collector{
	OpsTotal, OpDuration, ChangeStreamLag, ChangeStreamReconnects,
	StorageBytes, CurrentRevisionGauge, CompactedRevisionGauge,
}

// registrarMetricas registra os colecionadores. Erros de registro duplicado
// são ignorados: em testes o backend é construído várias vezes no mesmo
// processo, e isso não é motivo para falhar.
func registrarMetricas(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	for _, c := range colecionadores {
		if err := reg.Register(c); err != nil {
			if _, dup := err.(prometheus.AlreadyRegisteredError); !dup {
				logrus.Warnf("Falha ao registrar métrica do MongoDB: %v", err)
			}
		}
	}

	// Pré-inicializa as séries de labels conhecidos. Sem isso o Prometheus não
	// expõe um vetor vazio, e o dashboard mostra "no data" até o primeiro
	// evento acontecer — o que, no caso das reconexões, é justamente o que se
	// espera nunca acontecer.
	for _, motivo := range []string{"falha_ao_abrir", "historico_perdido", "queda"} {
		ChangeStreamReconnects.WithLabelValues(motivo)
	}
	for _, comp := range []string{"dados", "storage", "indices"} {
		StorageBytes.WithLabelValues(comp)
	}
}

// observar mede uma operação e registra resultado e duração.
func observar(op string, inicio time.Time, err error) {
	res := "success"
	if err != nil {
		res = "error"
	}
	OpsTotal.WithLabelValues(op, res).Inc()
	OpDuration.WithLabelValues(op).Observe(time.Since(inicio).Seconds())
}
