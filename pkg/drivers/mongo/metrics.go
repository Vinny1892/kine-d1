package mongo

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	OpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_mongo_ops_total",
		Help: "Total MongoDB operations by type and result",
	}, []string{"op", "result"})

	OpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kine_mongo_op_duration_seconds",
		Help:    "Duration of MongoDB operations",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"op"})

	ChangeStreamLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_mongo_change_stream_lag_seconds",
		Help: "Age of the last event received from the change stream",
	})

	ChangeStreamReconnects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_mongo_change_stream_reconnects_total",
		Help: "Change stream reconnections, by reason",
	}, []string{"motivo"})

	StorageBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kine_mongo_storage_bytes",
		Help: "Bytes used in MongoDB, by component",
	}, []string{"componente"})

	CurrentRevisionGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_mongo_current_revision",
		Help: "Current revision known to the backend",
	})

	CompactedRevisionGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_mongo_compacted_revision",
		Help: "Revision up to which the backend has compacted",
	})
)

var collectors = []prometheus.Collector{
	OpsTotal, OpDuration, ChangeStreamLag, ChangeStreamReconnects,
	StorageBytes, CurrentRevisionGauge, CompactedRevisionGauge,
}

// registerMetrics registers the collectors, ignoring duplicate registration.
func registerMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			if _, dup := err.(prometheus.AlreadyRegisteredError); !dup {
				logrus.Warnf("Failed to register MongoDB metric: %v", err)
			}
		}
	}

	for _, motivo := range []string{"falha_ao_abrir", "historico_perdido", "queda"} {
		ChangeStreamReconnects.WithLabelValues(motivo)
	}
	for _, comp := range []string{"dados", "storage", "indices"} {
		StorageBytes.WithLabelValues(comp)
	}
}

// observe records an operation's result and duration.
func observe(op string, start time.Time, err error) {
	res := "success"
	if err != nil {
		res = "error"
	}
	OpsTotal.WithLabelValues(op, res).Inc()
	OpDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}
