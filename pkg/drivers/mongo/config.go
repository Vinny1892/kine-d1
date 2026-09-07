package mongo

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config holds the driver options, extracted from the DSN.
type Config struct {
	URI string

	Database string

	Collection string

	EpochBase int64

	ConnectTimeout time.Duration

	ServerSelectionTimeout time.Duration
}

const (
	defaultDatabase   = "kine"
	defaultCollection = "kine"

	defaultEpochBase = 1767225600
)

// ParseDSN separates the kine_* parameters from the MongoDB connection string.
func ParseDSN(dsn string) (*Config, error) {
	if !strings.HasPrefix(dsn, "mongodb://") && !strings.HasPrefix(dsn, "mongodb+srv://") {
		return nil, fmt.Errorf("DSN must start with mongodb:// or mongodb+srv://, got %q", dsn)
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}

	cfg := &Config{
		Database:               defaultDatabase,
		Collection:             defaultCollection,
		EpochBase:              defaultEpochBase,
		ConnectTimeout:         30 * time.Second,
		ServerSelectionTimeout: 30 * time.Second,
	}

	q := u.Query()
	clean := url.Values{}
	for key, vals := range q {
		if !strings.HasPrefix(key, "kine_") {
			clean[key] = vals
			continue
		}
		if len(vals) == 0 {
			continue
		}
		v := vals[0]
		switch key {
		case "kine_database":
			cfg.Database = v
		case "kine_collection":
			cfg.Collection = v
		case "kine_epoch_base":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid kine_epoch_base %q: %w", v, err)
			}
			cfg.EpochBase = n
		case "kine_connect_timeout":
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid kine_connect_timeout %q: %w", v, err)
			}
			cfg.ConnectTimeout = d
		case "kine_server_selection_timeout":
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid kine_server_selection_timeout %q: %w", v, err)
			}
			cfg.ServerSelectionTimeout = d
		default:
			return nil, fmt.Errorf("unknown kine_ parameter: %q", key)
		}
	}

	if path := strings.TrimPrefix(u.Path, "/"); path != "" && !q.Has("kine_database") {
		cfg.Database = path
	}

	u.RawQuery = clean.Encode()
	cfg.URI = u.String()
	return cfg, nil
}
