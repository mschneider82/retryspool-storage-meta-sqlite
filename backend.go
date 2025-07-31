package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // SQLite driver
	metastorage "schneider.vip/retryspool/storage/meta"
)

// writeOperation represents a write operation to be executed serially
type writeOperation struct {
	opType   string
	ctx      context.Context
	resultCh chan writeResult
	// StoreMeta fields
	messageID string
	metadata  metastorage.MessageMetadata
	// DeleteMeta fields (messageID already covered)
	// MoveToState fields
	fromState metastorage.QueueState
	newState  metastorage.QueueState
}

type writeResult struct {
	err error
}

// Backend implements metastorage.Backend using SQLite with write serialization
type Backend struct {
	db       *sql.DB
	config   *Config
	writeCh  chan writeOperation
	stopCh   chan struct{}
	wg       sync.WaitGroup
	closed   bool
	closeMux sync.RWMutex
}

// NewBackend creates a new SQLite metadata storage backend
func NewBackend(config *Config) (*Backend, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(config.DatabasePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Build connection string with options
	dsn := config.DatabasePath
	if config.Options != nil {
		params := make([]string, 0)
		
		if config.Options.BusyTimeout > 0 {
			params = append(params, fmt.Sprintf("_busy_timeout=%d", config.Options.BusyTimeout))
		}
		if config.Options.CacheSize > 0 {
			params = append(params, fmt.Sprintf("_cache_size=-%d", config.Options.CacheSize)) // Negative for KB
		}
		if config.Options.ForeignKeys {
			params = append(params, "_foreign_keys=on")
		}
		if config.Options.JournalMode != "" {
			params = append(params, fmt.Sprintf("_journal_mode=%s", config.Options.JournalMode))
		}
		if config.Options.Synchronous != "" {
			params = append(params, fmt.Sprintf("_synchronous=%s", config.Options.Synchronous))
		}
		if config.Options.AutoVacuum != "" {
			params = append(params, fmt.Sprintf("_auto_vacuum=%s", config.Options.AutoVacuum))
		}
		if config.Options.TempStore != "" {
			params = append(params, fmt.Sprintf("_temp_store=%s", config.Options.TempStore))
		}
		
		if len(params) > 0 {
			dsn += "?" + strings.Join(params, "&")
		}
	}

	// Open database connection
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	if config.Options != nil {
		if config.Options.MaxConnections > 0 {
			db.SetMaxOpenConns(config.Options.MaxConnections)
		}
		if config.Options.MaxIdleConns > 0 {
			db.SetMaxIdleConns(config.Options.MaxIdleConns)
		}
		if config.Options.ConnMaxLifetime > 0 {
			db.SetConnMaxLifetime(time.Duration(config.Options.ConnMaxLifetime) * time.Second)
		}
	}

	backend := &Backend{
		db:      db,
		config:  config,
		writeCh: make(chan writeOperation, 100), // Buffered channel for write operations
		stopCh:  make(chan struct{}),
	}

	// Initialize database schema
	if err := backend.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Start the write worker goroutine
	backend.wg.Add(1)
	go backend.writeWorker()

	return backend, nil
}

// initSchema creates the necessary tables and indexes
func (b *Backend) initSchema() error {
	schema := `
	-- Messages table for storing message metadata
	CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY,
		state INTEGER NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL,
		next_retry INTEGER NOT NULL, -- Unix timestamp
		created INTEGER NOT NULL,    -- Unix timestamp
		updated INTEGER NOT NULL,    -- Unix timestamp
		last_error TEXT,
		size INTEGER NOT NULL DEFAULT 0,
		priority INTEGER NOT NULL DEFAULT 5,
		headers TEXT NOT NULL DEFAULT '{}' -- JSON encoded headers
	);

	-- Index for efficient state-based queries
	CREATE INDEX IF NOT EXISTS idx_messages_state ON messages(state, created);
	CREATE INDEX IF NOT EXISTS idx_messages_state_priority ON messages(state, priority DESC, created);
	CREATE INDEX IF NOT EXISTS idx_messages_next_retry ON messages(next_retry) WHERE state = 2; -- StateDeferred
	CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created);
	CREATE INDEX IF NOT EXISTS idx_messages_updated ON messages(updated);

	-- Enable WAL mode if configured
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA foreign_keys = ON;
	`

	_, err := b.db.Exec(schema)
	return err
}

// writeWorker processes all write operations serially to avoid database locking
func (b *Backend) writeWorker() {
	defer b.wg.Done()
	
	for {
		select {
		case <-b.stopCh:
			return
		case op := <-b.writeCh:
			var err error
			
			switch op.opType {
			case "store":
				err = b.doStoreMeta(op.ctx, op.messageID, op.metadata)
			case "delete":
				err = b.doDeleteMeta(op.ctx, op.messageID)
			case "move":
				err = b.doMoveToState(op.ctx, op.messageID, op.fromState, op.newState)
			default:
				err = fmt.Errorf("unknown operation type: %s", op.opType)
			}
			
			// Send result back
			op.resultCh <- writeResult{err: err}
		}
	}
}

// StoreMeta stores message metadata using write serialization
func (b *Backend) StoreMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	b.closeMux.RLock()
	if b.closed {
		b.closeMux.RUnlock()
		return fmt.Errorf("backend is closed")
	}
	b.closeMux.RUnlock()

	resultCh := make(chan writeResult, 1)
	op := writeOperation{
		opType:    "store",
		ctx:       ctx,
		resultCh:  resultCh,
		messageID: messageID,
		metadata:  metadata,
	}

	select {
	case b.writeCh <- op:
		// Wait for result
		result := <-resultCh
		return result.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// doStoreMeta performs the actual store operation (called by write worker)
func (b *Backend) doStoreMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	// Serialize headers to JSON
	headersJSON, err := json.Marshal(metadata.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	query := `
		INSERT OR REPLACE INTO messages (
			id, state, attempts, max_attempts, next_retry, created, updated,
			last_error, size, priority, headers
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err = b.db.ExecContext(ctx, query,
		messageID,
		int(metadata.State),
		metadata.Attempts,
		metadata.MaxAttempts,
		metadata.NextRetry.Unix(),
		metadata.Created.Unix(),
		metadata.Updated.Unix(),
		metadata.LastError,
		metadata.Size,
		metadata.Priority,
		string(headersJSON),
	)

	if err != nil {
		return fmt.Errorf("failed to store metadata for message %s: %w", messageID, err)
	}

	return nil
}

// GetMeta retrieves message metadata
func (b *Backend) GetMeta(ctx context.Context, messageID string) (metastorage.MessageMetadata, error) {
	query := `
		SELECT state, attempts, max_attempts, next_retry, created, updated,
			   last_error, size, priority, headers
		FROM messages
		WHERE id = ?
	`

	var metadata metastorage.MessageMetadata
	var state int
	var nextRetryUnix, createdUnix, updatedUnix int64
	var headersJSON string

	err := b.db.QueryRowContext(ctx, query, messageID).Scan(
		&state,
		&metadata.Attempts,
		&metadata.MaxAttempts,
		&nextRetryUnix,
		&createdUnix,
		&updatedUnix,
		&metadata.LastError,
		&metadata.Size,
		&metadata.Priority,
		&headersJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return metastorage.MessageMetadata{}, fmt.Errorf("message %s not found", messageID)
		}
		return metastorage.MessageMetadata{}, fmt.Errorf("failed to get metadata for message %s: %w", messageID, err)
	}

	// Convert timestamps
	metadata.State = metastorage.QueueState(state)
	metadata.NextRetry = time.Unix(nextRetryUnix, 0)
	metadata.Created = time.Unix(createdUnix, 0)
	metadata.Updated = time.Unix(updatedUnix, 0)

	// Deserialize headers
	if err := json.Unmarshal([]byte(headersJSON), &metadata.Headers); err != nil {
		return metastorage.MessageMetadata{}, fmt.Errorf("failed to unmarshal headers for message %s: %w", messageID, err)
	}

	metadata.ID = messageID
	return metadata, nil
}

// UpdateMeta updates message metadata
func (b *Backend) UpdateMeta(ctx context.Context, messageID string, metadata metastorage.MessageMetadata) error {
	return b.StoreMeta(ctx, messageID, metadata)
}

// DeleteMeta removes message metadata using write serialization
func (b *Backend) DeleteMeta(ctx context.Context, messageID string) error {
	b.closeMux.RLock()
	if b.closed {
		b.closeMux.RUnlock()
		return fmt.Errorf("backend is closed")
	}
	b.closeMux.RUnlock()

	resultCh := make(chan writeResult, 1)
	op := writeOperation{
		opType:    "delete",
		ctx:       ctx,
		resultCh:  resultCh,
		messageID: messageID,
	}

	select {
	case b.writeCh <- op:
		// Wait for result
		result := <-resultCh
		return result.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// doDeleteMeta performs the actual delete operation (called by write worker)
func (b *Backend) doDeleteMeta(ctx context.Context, messageID string) error {
	query := `DELETE FROM messages WHERE id = ?`
	
	result, err := b.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("failed to delete metadata for message %s: %w", messageID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for message %s: %w", messageID, err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("message %s not found", messageID)
	}

	return nil
}

// ListMessages lists messages with pagination and filtering
func (b *Backend) ListMessages(ctx context.Context, state metastorage.QueueState, options metastorage.MessageListOptions) (metastorage.MessageListResult, error) {
	// Build query with filters
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "state = ?")
	args = append(args, int(state))

	if !options.Since.IsZero() {
		conditions = append(conditions, "created >= ?")
		args = append(args, options.Since.Unix())
	}

	whereClause := "WHERE " + strings.Join(conditions, " AND ")

	// Build ORDER BY clause
	orderBy := "ORDER BY created ASC" // Default
	if options.SortBy != "" {
		sortField := options.SortBy
		switch sortField {
		case "created", "updated", "priority", "attempts":
			// Valid sort fields
		default:
			sortField = "created"
		}

		sortOrder := "ASC"
		if options.SortOrder == "desc" {
			sortOrder = "DESC"
		}

		orderBy = fmt.Sprintf("ORDER BY %s %s", sortField, sortOrder)
	}

	// Count total messages
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM messages %s", whereClause)
	var total int
	err := b.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return metastorage.MessageListResult{}, fmt.Errorf("failed to count messages: %w", err)
	}

	// Build main query with pagination
	query := fmt.Sprintf(`
		SELECT id FROM messages
		%s
		%s
		LIMIT ? OFFSET ?
	`, whereClause, orderBy)

	limit := options.Limit
	if limit <= 0 {
		limit = 25 // Default limit
	}

	args = append(args, limit, options.Offset)

	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return metastorage.MessageListResult{}, fmt.Errorf("failed to list messages: %w", err)
	}
	defer rows.Close()

	var messageIDs []string
	for rows.Next() {
		var messageID string
		if err := rows.Scan(&messageID); err != nil {
			return metastorage.MessageListResult{}, fmt.Errorf("failed to scan message ID: %w", err)
		}
		messageIDs = append(messageIDs, messageID)
	}

	if err := rows.Err(); err != nil {
		return metastorage.MessageListResult{}, fmt.Errorf("error iterating rows: %w", err)
	}

	hasMore := options.Offset+len(messageIDs) < total

	return metastorage.MessageListResult{
		MessageIDs: messageIDs,
		Total:      total,
		HasMore:    hasMore,
	}, nil
}

// NewMessageIterator creates an iterator for messages in a specific state
func (b *Backend) NewMessageIterator(ctx context.Context, state metastorage.QueueState, batchSize int) (metastorage.MessageIterator, error) {
	b.closeMux.RLock()
	if b.closed {
		b.closeMux.RUnlock()
		return nil, fmt.Errorf("backend is closed")
	}
	b.closeMux.RUnlock()

	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	return &sqliteIterator{
		backend:   b,
		state:     state,
		batchSize: batchSize,
		ctx:       ctx,
		offset:    0,
	}, nil
}

// sqliteIterator implements MessageIterator for SQLite backend
type sqliteIterator struct {
	backend   *Backend
	state     metastorage.QueueState
	batchSize int
	ctx       context.Context
	
	// Current state
	current   []metastorage.MessageMetadata
	index     int
	offset    int
	finished  bool
	closed    bool
	mu        sync.RWMutex
}

// Next returns the next message metadata
func (it *sqliteIterator) Next(ctx context.Context) (metastorage.MessageMetadata, bool, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return metastorage.MessageMetadata{}, false, fmt.Errorf("iterator is closed")
	}

	// Initialize or load next batch if needed
	if it.current == nil || (it.index >= len(it.current) && !it.finished) {
		if err := it.loadBatch(ctx); err != nil {
			return metastorage.MessageMetadata{}, false, err
		}
	}

	// Check if we're at the end
	if it.index >= len(it.current) {
		return metastorage.MessageMetadata{}, false, nil
	}

	// Return current message and advance
	metadata := it.current[it.index]
	it.index++

	// Check if there are more messages
	hasMore := it.index < len(it.current) || !it.finished

	return metadata, hasMore, nil
}

// loadBatch loads the next batch of messages from SQLite
func (it *sqliteIterator) loadBatch(ctx context.Context) error {
	if it.finished {
		return nil
	}

	// Build query for full metadata
	query := `
		SELECT id, state, attempts, max_attempts, next_retry, created, updated, 
			   last_error, size, priority, headers
		FROM messages 
		WHERE state = ? 
		ORDER BY created ASC 
		LIMIT ? OFFSET ?
	`

	rows, err := it.backend.db.QueryContext(ctx, query, int(it.state), it.batchSize, it.offset)
	if err != nil {
		return fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	// Load batch into memory
	var batch []metastorage.MessageMetadata
	for rows.Next() {
		var metadata metastorage.MessageMetadata
		var headersJSON []byte
		var nextRetry, created, updated int64

		err := rows.Scan(
			&metadata.ID,
			&metadata.State,
			&metadata.Attempts,
			&metadata.MaxAttempts,
			&nextRetry,
			&created,
			&updated,
			&metadata.LastError,
			&metadata.Size,
			&metadata.Priority,
			&headersJSON,
		)
		if err != nil {
			return fmt.Errorf("failed to scan message: %w", err)
		}

		// Convert Unix timestamps to time.Time
		metadata.NextRetry = time.Unix(nextRetry, 0)
		metadata.Created = time.Unix(created, 0)
		metadata.Updated = time.Unix(updated, 0)

		// Unmarshal headers
		if len(headersJSON) > 0 {
			if err := json.Unmarshal(headersJSON, &metadata.Headers); err != nil {
				return fmt.Errorf("failed to unmarshal headers: %w", err)
			}
		}

		batch = append(batch, metadata)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	// Update iterator state
	it.current = batch
	it.index = 0
	it.offset += len(batch)

	// Check if we're finished (batch smaller than requested)
	if len(batch) < it.batchSize {
		it.finished = true
	}

	return nil
}

// Close closes the iterator
func (it *sqliteIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	
	it.closed = true
	it.current = nil
	return nil
}

// MoveToState moves a message to a different queue state using write serialization
func (b *Backend) MoveToState(ctx context.Context, messageID string, fromState, toState metastorage.QueueState) error {
	b.closeMux.RLock()
	if b.closed {
		b.closeMux.RUnlock()
		return fmt.Errorf("backend is closed")
	}
	b.closeMux.RUnlock()

	resultCh := make(chan writeResult, 1)
	op := writeOperation{
		opType:    "move",
		ctx:       ctx,
		resultCh:  resultCh,
		messageID: messageID,
		fromState: fromState,
		newState:  toState,
	}

	select {
	case b.writeCh <- op:
		// Wait for result
		result := <-resultCh
		return result.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// doMoveToState performs the actual move operation (called by write worker)
func (b *Backend) doMoveToState(ctx context.Context, messageID string, fromState, toState metastorage.QueueState) error {
	// Use a transaction to ensure atomicity
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if tx.Commit() succeeds

	// Get current metadata
	query := `
		SELECT state, attempts, max_attempts, next_retry, created, updated,
			   last_error, size, priority, headers
		FROM messages
		WHERE id = ?
	`

	var metadata metastorage.MessageMetadata
	var state int
	var nextRetryUnix, createdUnix, updatedUnix int64
	var headersJSON string

	err = tx.QueryRowContext(ctx, query, messageID).Scan(
		&state,
		&metadata.Attempts,
		&metadata.MaxAttempts,
		&nextRetryUnix,
		&createdUnix,
		&updatedUnix,
		&metadata.LastError,
		&metadata.Size,
		&metadata.Priority,
		&headersJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("message %s not found", messageID)
		}
		return fmt.Errorf("failed to get metadata for message %s: %w", messageID, err)
	}

	// Convert timestamps and deserialize headers
	metadata.State = metastorage.QueueState(state)
	metadata.NextRetry = time.Unix(nextRetryUnix, 0)
	metadata.Created = time.Unix(createdUnix, 0)
	metadata.Updated = time.Unix(updatedUnix, 0)
	metadata.ID = messageID

	if err := json.Unmarshal([]byte(headersJSON), &metadata.Headers); err != nil {
		return fmt.Errorf("failed to unmarshal headers for message %s: %w", messageID, err)
	}

	// Atomic check: ensure message is in expected fromState
	if metadata.State != fromState {
		return fmt.Errorf("message %s is in state %d, expected %d", messageID, int(metadata.State), int(fromState))
	}

	// Update state and timestamp
	metadata.State = toState
	metadata.Updated = time.Now()

	// Serialize headers
	headersJSON2, err := json.Marshal(metadata.Headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}

	// Atomic update: only move if still in expected fromState
	updateQuery := `
		UPDATE messages
		SET state = ?, updated = ?, headers = ?
		WHERE id = ? AND state = ?
	`

	result, err := tx.ExecContext(ctx, updateQuery,
		int(metadata.State),
		metadata.Updated.Unix(),
		string(headersJSON2),
		messageID,
		int(fromState),
	)

	if err != nil {
		return fmt.Errorf("failed to update state for message %s: %w", messageID, err)
	}

	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("message %s state changed during operation", messageID)
	}

	if err != nil {
		return fmt.Errorf("failed to update state for message %s: %w", messageID, err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Close closes the database connection and stops the write worker
func (b *Backend) Close() error {
	b.closeMux.Lock()
	if b.closed {
		b.closeMux.Unlock()
		return nil
	}
	b.closed = true
	b.closeMux.Unlock()

	// Stop the write worker
	close(b.stopCh)
	b.wg.Wait()

	// Close the database
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}