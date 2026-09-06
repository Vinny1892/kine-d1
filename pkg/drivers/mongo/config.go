package mongo

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config carrega as opções do driver, extraídas da DSN.
//
// A DSN é uma connection string normal do MongoDB, com parâmetros extras
// prefixados por "kine_" que são removidos antes de repassá-la ao driver:
//
//	mongodb+srv://user:pass@host/kine?kine_database=kine&kine_compact_interval=5m
type Config struct {
	// URI é a connection string já limpa dos parâmetros kine_*.
	URI string

	// Database é o banco onde as coleções vivem. Default: "kine".
	Database string

	// Collection é a coleção do log de revisões. Default: "kine".
	Collection string

	// EpochBase é o instante (unix seconds) a partir do qual as revisões são
	// contadas. Ver revision.go: sem uma base, o clusterTime deslocado de 32
	// bits estoura o int64 por volta de 2038.
	//
	// É gravado nos metadados do cluster na primeira execução e NUNCA pode
	// mudar depois — alterá-lo reescreveria o significado de toda revisão já
	// entregue ao apiserver.
	EpochBase int64

	// ConnectTimeout limita o tempo de estabelecimento da conexão inicial.
	ConnectTimeout time.Duration

	// ServerSelectionTimeout limita a espera por um servidor elegível.
	ServerSelectionTimeout time.Duration
}

const (
	defaultDatabase   = "kine"
	defaultCollection = "kine"

	// defaultEpochBase é 2026-01-01T00:00:00Z. Serve apenas como valor
	// inicial; o valor efetivo é o que estiver gravado nos metadados.
	defaultEpochBase = 1767225600
)

// ParseDSN separa os parâmetros kine_* da connection string do MongoDB.
//
// O driver oficial rejeita parâmetros desconhecidos, então eles precisam ser
// removidos da URI antes de repassá-la.
func ParseDSN(dsn string) (*Config, error) {
	if !strings.HasPrefix(dsn, "mongodb://") && !strings.HasPrefix(dsn, "mongodb+srv://") {
		return nil, fmt.Errorf("DSN deve começar com mongodb:// ou mongodb+srv://, recebido %q", dsn)
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("DSN inválida: %w", err)
	}

	cfg := &Config{
		Database:               defaultDatabase,
		Collection:             defaultCollection,
		EpochBase:              defaultEpochBase,
		ConnectTimeout:         30 * time.Second,
		ServerSelectionTimeout: 30 * time.Second,
	}

	q := u.Query()
	limpa := url.Values{}
	for chave, vals := range q {
		if !strings.HasPrefix(chave, "kine_") {
			limpa[chave] = vals
			continue
		}
		if len(vals) == 0 {
			continue
		}
		v := vals[0]
		switch chave {
		case "kine_database":
			cfg.Database = v
		case "kine_collection":
			cfg.Collection = v
		case "kine_epoch_base":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("kine_epoch_base inválido %q: %w", v, err)
			}
			cfg.EpochBase = n
		case "kine_connect_timeout":
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("kine_connect_timeout inválido %q: %w", v, err)
			}
			cfg.ConnectTimeout = d
		case "kine_server_selection_timeout":
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("kine_server_selection_timeout inválido %q: %w", v, err)
			}
			cfg.ServerSelectionTimeout = d
		default:
			return nil, fmt.Errorf("parâmetro kine_ desconhecido: %q", chave)
		}
	}

	// O nome do banco também pode vir no caminho da URI. O parâmetro
	// kine_database, quando presente, tem precedência.
	if path := strings.TrimPrefix(u.Path, "/"); path != "" && !q.Has("kine_database") {
		cfg.Database = path
	}

	u.RawQuery = limpa.Encode()
	cfg.URI = u.String()
	return cfg, nil
}
