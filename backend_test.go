package sqlite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	metastorage "schneider.vip/retryspool/storage/meta"
)

func TestSQLiteMetaBackend(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite-meta-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create factory
	factory := NewFactory(
		WithDatabasePath(dbPath),
		WithWALMode(true),
		WithBusyTimeout(5000),
		WithCacheSize(1000),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Test metadata
	messageID := "test-message-123"
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 3,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		LastError:   "",
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":      "user@example.com",
			"from":    "sender@example.com",
			"subject": "Test Message",
		},
	}

	// Test StoreMeta
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store metadata: %v", err)
	}

	// Test GetMeta
	retrievedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	// Verify metadata
	if retrievedMeta.ID != metadata.ID {
		t.Errorf("ID mismatch: expected %s, got %s", metadata.ID, retrievedMeta.ID)
	}
	if retrievedMeta.State != metadata.State {
		t.Errorf("State mismatch: expected %v, got %v", metadata.State, retrievedMeta.State)
	}
	if retrievedMeta.Headers["subject"] != metadata.Headers["subject"] {
		t.Errorf("Header mismatch: expected %s, got %s", metadata.Headers["subject"], retrievedMeta.Headers["subject"])
	}

	// Test UpdateMeta - change state
	metadata.State = metastorage.StateActive
	metadata.Attempts = 1
	metadata.Updated = time.Now()
	
	err = backend.UpdateMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to update metadata: %v", err)
	}

	// Verify update
	updatedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get updated metadata: %v", err)
	}
	if updatedMeta.State != metastorage.StateActive {
		t.Errorf("State not updated: expected %v, got %v", metastorage.StateActive, updatedMeta.State)
	}
	if updatedMeta.Attempts != 1 {
		t.Errorf("Attempts not updated: expected 1, got %d", updatedMeta.Attempts)
	}

	// Test MoveToState with transaction
	err = backend.MoveToState(ctx, messageID, metastorage.StateActive, metastorage.StateDeferred)
	if err != nil {
		t.Fatalf("Failed to move to state: %v", err)
	}

	movedMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get moved metadata: %v", err)
	}
	if movedMeta.State != metastorage.StateDeferred {
		t.Errorf("State not moved: expected %v, got %v", metastorage.StateDeferred, movedMeta.State)
	}

	// Test ListMessages
	result, err := backend.ListMessages(ctx, metastorage.StateDeferred, metastorage.MessageListOptions{
		Limit:     10,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}

	if len(result.MessageIDs) != 1 {
		t.Errorf("Expected 1 message in deferred state, got %d", len(result.MessageIDs))
	}
	if len(result.MessageIDs) > 0 && result.MessageIDs[0] != messageID {
		t.Errorf("Expected message ID %s, got %s", messageID, result.MessageIDs[0])
	}
	if result.Total != 1 {
		t.Errorf("Expected total 1, got %d", result.Total)
	}

	// Test DeleteMeta
	err = backend.DeleteMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to delete metadata: %v", err)
	}

	// Verify deletion
	_, err = backend.GetMeta(ctx, messageID)
	if err == nil {
		t.Error("Expected error when getting deleted metadata")
	}

	t.Log("All SQLite metadata backend tests passed!")
}

func TestSQLiteMetaBackendMultipleMessages(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite-meta-multi-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test-multi.db")

	// Create factory with different options
	factory := NewFactory(
		WithDatabasePath(dbPath),
		WithWALMode(true),
		WithJournalMode("WAL"),
		WithSynchronous("NORMAL"),
		WithMaxConnections(5),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Create multiple messages in different states
	messages := []struct {
		id    string
		state metastorage.QueueState
	}{
		{"msg-1", metastorage.StateIncoming},
		{"msg-2", metastorage.StateIncoming},
		{"msg-3", metastorage.StateActive},
		{"msg-4", metastorage.StateDeferred},
		{"msg-5", metastorage.StateDeferred},
	}

	// Store all messages
	for i, msg := range messages {
		metadata := metastorage.MessageMetadata{
			ID:          msg.id,
			State:       msg.state,
			Attempts:    0,
			MaxAttempts: 3,
			NextRetry:   time.Now().Add(time.Hour),
			Created:     time.Now().Add(time.Duration(i) * time.Second), // Different timestamps
			Updated:     time.Now(),
			Size:        1024,
			Priority:    5,
			Headers: map[string]string{
				"to":   "user@example.com",
				"from": "sender@example.com",
			},
		}

		err = backend.StoreMeta(ctx, msg.id, metadata)
		if err != nil {
			t.Fatalf("Failed to store metadata for %s: %v", msg.id, err)
		}
	}

	// Test listing incoming messages
	result, err := backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		Limit:     10,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list incoming messages: %v", err)
	}

	if len(result.MessageIDs) != 2 {
		t.Errorf("Expected 2 incoming messages, got %d", len(result.MessageIDs))
	}
	if result.Total != 2 {
		t.Errorf("Expected total 2, got %d", result.Total)
	}

	// Test listing deferred messages
	result, err = backend.ListMessages(ctx, metastorage.StateDeferred, metastorage.MessageListOptions{
		Limit:     10,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list deferred messages: %v", err)
	}

	if len(result.MessageIDs) != 2 {
		t.Errorf("Expected 2 deferred messages, got %d", len(result.MessageIDs))
	}

	// Test pagination
	result, err = backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		Limit:     1,
		Offset:    0,
		SortBy:    "created",
		SortOrder: "asc",
	})
	if err != nil {
		t.Fatalf("Failed to list messages with pagination: %v", err)
	}

	if len(result.MessageIDs) != 1 {
		t.Errorf("Expected 1 message with limit=1, got %d", len(result.MessageIDs))
	}
	if !result.HasMore {
		t.Error("Expected HasMore=true with pagination")
	}

	// Test sorting by priority
	result, err = backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		Limit:     10,
		Offset:    0,
		SortBy:    "priority",
		SortOrder: "desc",
	})
	if err != nil {
		t.Fatalf("Failed to list messages sorted by priority: %v", err)
	}

	if len(result.MessageIDs) != 2 {
		t.Errorf("Expected 2 messages sorted by priority, got %d", len(result.MessageIDs))
	}

	// Clean up
	for _, msg := range messages {
		backend.DeleteMeta(ctx, msg.id)
	}

	t.Log("Multiple messages test passed!")
}

func TestSQLiteMetaBackendConcurrency(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite-meta-concurrency-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test-concurrency.db")

	// Create factory with WAL mode for better concurrency
	factory := NewFactory(
		WithDatabasePath(dbPath),
		WithWALMode(true),
		WithBusyTimeout(10000), // 10 seconds
		WithMaxConnections(10),
		WithMaxIdleConns(5),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Test concurrent state transitions
	messageID := "concurrent-test-message"
	metadata := metastorage.MessageMetadata{
		ID:          messageID,
		State:       metastorage.StateIncoming,
		Attempts:    0,
		MaxAttempts: 3,
		NextRetry:   time.Now().Add(time.Hour),
		Created:     time.Now(),
		Updated:     time.Now(),
		Size:        1024,
		Priority:    5,
		Headers: map[string]string{
			"to":   "user@example.com",
			"from": "sender@example.com",
		},
	}

	// Store initial message
	err = backend.StoreMeta(ctx, messageID, metadata)
	if err != nil {
		t.Fatalf("Failed to store initial metadata: %v", err)
	}

	// Test concurrent MoveToState operations
	done := make(chan error, 2)

	// Goroutine 1: Move to Active
	go func() {
		err := backend.MoveToState(ctx, messageID, metastorage.StateIncoming, metastorage.StateActive)
		done <- err
	}()

	// Goroutine 2: Move to Deferred (should happen after the first one)
	go func() {
		time.Sleep(10 * time.Millisecond) // Small delay
		err := backend.MoveToState(ctx, messageID, metastorage.StateActive, metastorage.StateDeferred)
		done <- err
	}()

	// Wait for both operations to complete
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Errorf("Concurrent operation failed: %v", err)
		}
	}

	// Verify final state
	finalMeta, err := backend.GetMeta(ctx, messageID)
	if err != nil {
		t.Fatalf("Failed to get final metadata: %v", err)
	}

	// Should be in Deferred state (last operation)
	if finalMeta.State != metastorage.StateDeferred {
		t.Errorf("Expected final state to be Deferred, got %v", finalMeta.State)
	}

	// Clean up
	backend.DeleteMeta(ctx, messageID)

	t.Log("Concurrency test passed!")
}

func TestSQLiteMetaBackendErrorHandling(t *testing.T) {
	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite-meta-error-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test-error.db")

	// Create factory
	factory := NewFactory(
		WithDatabasePath(dbPath),
		WithWALMode(true),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Test MoveToState with non-existent message (should fail)
	err = backend.MoveToState(ctx, "non-existent-message", metastorage.StateIncoming, metastorage.StateActive)
	if err == nil {
		t.Error("Expected error when moving non-existent message")
	}

	t.Log("Error handling test passed!")
}

func TestSQLiteMetaBackendPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create temporary database file
	tmpDir, err := os.MkdirTemp("", "sqlite-meta-perf-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test-perf.db")

	// Create factory optimized for performance
	factory := NewFactory(
		WithDatabasePath(dbPath),
		WithWALMode(true),
		WithSynchronous("NORMAL"),
		WithCacheSize(10000), // 10MB cache
		WithMaxConnections(10),
	)

	// Create backend
	backend, err := factory.Create()
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Insert many messages
	numMessages := 1000
	start := time.Now()

	for i := 0; i < numMessages; i++ {
		messageID := fmt.Sprintf("perf-test-message-%d", i)
		metadata := metastorage.MessageMetadata{
			ID:          messageID,
			State:       metastorage.QueueState(i % 5), // Distribute across states
			Attempts:    0,
			MaxAttempts: 3,
			NextRetry:   time.Now().Add(time.Hour),
			Created:     time.Now(),
			Updated:     time.Now(),
			Size:        1024,
			Priority:    i % 10, // Different priorities
			Headers: map[string]string{
				"to":   "user@example.com",
				"from": "sender@example.com",
				"id":   fmt.Sprintf("%d", i),
			},
		}

		err = backend.StoreMeta(ctx, messageID, metadata)
		if err != nil {
			t.Fatalf("Failed to store metadata for message %d: %v", i, err)
		}
	}

	insertDuration := time.Since(start)
	t.Logf("Inserted %d messages in %v (%.2f msg/sec)", numMessages, insertDuration, float64(numMessages)/insertDuration.Seconds())

	// Test query performance
	start = time.Now()
	result, err := backend.ListMessages(ctx, metastorage.StateIncoming, metastorage.MessageListOptions{
		Limit:     100,
		Offset:    0,
		SortBy:    "priority",
		SortOrder: "desc",
	})
	if err != nil {
		t.Fatalf("Failed to list messages: %v", err)
	}
	queryDuration := time.Since(start)
	t.Logf("Listed %d messages in %v", len(result.MessageIDs), queryDuration)

	// Test update performance
	start = time.Now()
	for i := 0; i < 100; i++ {
		messageID := fmt.Sprintf("perf-test-message-%d", i)
		err = backend.MoveToState(ctx, messageID, metastorage.QueueState(i % 5), metastorage.StateActive)
		if err != nil {
			t.Fatalf("Failed to move message %d to active: %v", i, err)
		}
	}
	updateDuration := time.Since(start)
	t.Logf("Updated 100 messages in %v (%.2f updates/sec)", updateDuration, 100.0/updateDuration.Seconds())

	t.Log("Performance test completed!")
}