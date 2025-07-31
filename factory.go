package sqlite

import (
	metastorage "schneider.vip/retryspool/storage/meta"
)

// Factory implements metastorage.Factory for SQLite storage
type Factory struct {
	config *Config
}

// Config holds SQLite database configuration
type Config struct {
	DatabasePath string // Path to SQLite database file
	// SQLite specific options
	Options *SQLiteOptions
}

// SQLiteOptions holds SQLite specific configuration
type SQLiteOptions struct {
	WALMode          bool   // Enable WAL mode for better concurrency
	BusyTimeout      int    // Busy timeout in milliseconds (default: 30000)
	CacheSize        int    // Cache size in KB (default: 2000)
	JournalMode      string // Journal mode: DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF
	Synchronous      string // Synchronous mode: OFF, NORMAL, FULL, EXTRA
	ForeignKeys      bool   // Enable foreign key constraints
	AutoVacuum       string // Auto vacuum mode: NONE, FULL, INCREMENTAL
	TempStore        string // Temp store: DEFAULT, FILE, MEMORY
	MaxConnections   int    // Maximum number of open connections (default: 1)
	MaxIdleConns     int    // Maximum number of idle connections (default: 1)
	ConnMaxLifetime  int    // Connection max lifetime in seconds (default: 0 = unlimited)
}

// Option configures the SQLite factory
type Option func(*Config)

// WithDatabasePath sets the SQLite database file path
func WithDatabasePath(path string) Option {
	return func(c *Config) {
		c.DatabasePath = path
	}
}

// WithSQLiteOptions sets SQLite specific options
func WithSQLiteOptions(opts *SQLiteOptions) Option {
	return func(c *Config) {
		c.Options = opts
	}
}

// WithWALMode enables WAL mode for better concurrency
func WithWALMode(enable bool) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.WALMode = enable
	}
}

// WithBusyTimeout sets the busy timeout in milliseconds
func WithBusyTimeout(timeout int) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.BusyTimeout = timeout
	}
}

// WithCacheSize sets the cache size in KB
func WithCacheSize(size int) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.CacheSize = size
	}
}

// WithJournalMode sets the journal mode
func WithJournalMode(mode string) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.JournalMode = mode
	}
}

// WithSynchronous sets the synchronous mode
func WithSynchronous(mode string) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.Synchronous = mode
	}
}

// WithForeignKeys enables foreign key constraints
func WithForeignKeys(enable bool) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.ForeignKeys = enable
	}
}

// WithAutoVacuum sets the auto vacuum mode
func WithAutoVacuum(mode string) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.AutoVacuum = mode
	}
}

// WithMaxConnections sets the maximum number of open connections
func WithMaxConnections(max int) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.MaxConnections = max
	}
}

// WithMaxIdleConns sets the maximum number of idle connections
func WithMaxIdleConns(max int) Option {
	return func(c *Config) {
		if c.Options == nil {
			c.Options = &SQLiteOptions{}
		}
		c.Options.MaxIdleConns = max
	}
}

// NewFactory creates a new SQLite metadata storage factory
func NewFactory(opts ...Option) *Factory {
	config := &Config{
		DatabasePath: "retryspool-meta.db",
		Options: &SQLiteOptions{
			WALMode:          true,   // Enable WAL for better concurrency
			BusyTimeout:      30000,  // 30 seconds
			CacheSize:        2000,   // 2MB cache
			JournalMode:      "WAL",  // Write-Ahead Logging
			Synchronous:      "NORMAL", // Good balance of safety and performance
			ForeignKeys:      true,   // Enable foreign keys
			AutoVacuum:       "INCREMENTAL", // Incremental auto vacuum
			TempStore:        "MEMORY", // Store temp tables in memory
			MaxConnections:   10,     // Allow multiple connections
			MaxIdleConns:     2,      // Keep some idle connections
			ConnMaxLifetime:  3600,   // 1 hour connection lifetime
		},
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	return &Factory{
		config: config,
	}
}

// Create creates a new SQLite metadata storage backend
func (f *Factory) Create() (metastorage.Backend, error) {
	return NewBackend(f.config)
}

// Name returns the factory name
func (f *Factory) Name() string {
	return "sqlite-meta"
}