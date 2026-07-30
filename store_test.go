package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func TestMigrateLegacyDatabaseWithCompanionFiles(t *testing.T) {
	directory := t.TempDir()
	legacyPath := filepath.Join(directory, "streamscope.db")
	currentPath := filepath.Join(directory, "redisstreamscope.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.WriteFile(legacyPath+suffix, []byte("legacy"+suffix), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := migrateLegacyDatabase(legacyPath, currentPath); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(legacyPath + suffix); !os.IsNotExist(err) {
			t.Fatalf("legacy file %s was not moved", suffix)
		}
		content, err := os.ReadFile(currentPath + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if string(content) != "legacy"+suffix {
			t.Fatalf("migrated file %s has unexpected content", suffix)
		}
	}
}

func TestMigrateLegacyDatabaseDoesNotOverwriteCurrentDatabase(t *testing.T) {
	directory := t.TempDir()
	legacyPath := filepath.Join(directory, "streamscope.db")
	currentPath := filepath.Join(directory, "redisstreamscope.db")
	if err := os.WriteFile(legacyPath, []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(currentPath, []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := migrateLegacyDatabase(legacyPath, currentPath); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "current" {
		t.Fatal("current database was overwritten")
	}
	if _, err := os.Stat(legacyPath); err != nil {
		t.Fatal("legacy database should remain when the current database exists")
	}
}

func TestWildcardMatch(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"*", "stream:prod:orders.events", true},
		{"stream:prod:orders.*", "stream:prod:orders.events", true},
		{"stream:prod:orders.*", "stream:prod:payments.events", false},
		{"streams:read", "streams:read", true},
		{"streams:write", "streams:read", false},
	}
	for _, test := range tests {
		if got := wildcardMatch(test.pattern, test.value); got != test.want {
			t.Fatalf("wildcardMatch(%q, %q)=%v, want %v", test.pattern, test.value, got, test.want)
		}
	}
}

func TestStoreUsersSessionsPermissionsAndLogs(t *testing.T) {
	config := appConfig{
		DataPath:   filepath.Join(t.TempDir(), "redisstreamscope.db"),
		SessionTTL: time.Hour,
	}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	ctx := context.Background()

	adminHash, _ := hashPassword("correct-horse-battery")
	admin, err := store.createInitialAdmin(ctx, "root-admin", "Root Admin", adminHash)
	if err != nil || admin.Role != "admin" || admin.PasswordChangeRequired {
		t.Fatalf("initial admin: user=%+v err=%v", admin, err)
	}
	viewerHash, _ := hashPassword("viewer-password-123")
	viewer, err := store.createUser(ctx, "viewer-one", "Viewer One", string(viewerHash), "viewer")
	if err != nil {
		t.Fatal(err)
	}
	updatedViewer, err := store.updateUser(ctx, viewer.ID, "viewer-renamed", "Viewer Renamed", "viewer", true, "")
	if err != nil || updatedViewer.Username != "viewer-renamed" || updatedViewer.DisplayName != "Viewer Renamed" || updatedViewer.Role != "viewer" {
		t.Fatalf("update user=%+v err=%v", updatedViewer, err)
	}
	viewer = updatedViewer
	if _, err := store.updateUser(ctx, admin.ID, admin.Username, admin.DisplayName, "viewer", true, ""); err == nil {
		t.Fatal("the last enabled administrator must not be demoted")
	}
	token, expires, err := store.createSession(ctx, viewer, time.Hour, "127.0.0.1", "test")
	if err != nil || token == "" || !expires.After(time.Now()) {
		t.Fatalf("create session token=%q expires=%v err=%v", token, expires, err)
	}
	session, err := store.session(ctx, token)
	if err != nil || session.Username != viewer.Username {
		t.Fatalf("resolve session=%+v err=%v", session, err)
	}
	if !session.PasswordChangeRequired {
		t.Fatal("new users must change their temporary password")
	}
	replacementHash, _ := hashPassword("viewer-replacement-123")
	if err := store.changeOwnPassword(ctx, viewer.ID, replacementHash); err != nil {
		t.Fatal(err)
	}
	if _, err := store.session(ctx, token); err == nil {
		t.Fatal("changing password must invalidate prior sessions")
	}
	viewer, _, err = store.authenticate(ctx, viewer.Username)
	if err != nil || viewer.PasswordChangeRequired {
		t.Fatalf("password change flag was not cleared: user=%+v err=%v", viewer, err)
	}
	token, _, err = store.createSession(ctx, viewer, time.Hour, "127.0.0.1", "test")
	if err != nil {
		t.Fatal(err)
	}
	session, err = store.session(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	if !store.allowed(ctx, session, "streams:read", "stream:prod:orders.events") {
		t.Fatal("viewer should have stream read")
	}
	if store.allowed(ctx, session, "streams:write", "stream:prod:orders.events") {
		t.Fatal("viewer should not have stream write by default")
	}
	if store.allowed(ctx, session, "users:read", "users:*") {
		t.Fatal("viewer should not read users")
	}
	_, err = store.upsertGrant(ctx, grantRecord{UserID: viewer.ID, Action: "streams:write", Scope: "stream:prod:orders.*", Effect: "allow"})
	if err != nil || !store.allowed(ctx, session, "streams:write", "stream:prod:orders.events") {
		t.Fatalf("grant stream write err=%v", err)
	}
	_, err = store.upsertGrant(ctx, grantRecord{UserID: viewer.ID, Action: "streams:write", Scope: "stream:prod:orders.events", Effect: "deny"})
	if err != nil || store.allowed(ctx, session, "streams:write", "stream:prod:orders.events") {
		t.Fatalf("explicit deny should win err=%v", err)
	}
	grant, err := store.upsertGrant(ctx, grantRecord{UserID: viewer.ID, Action: "groups:manage", Scope: "stream:prod:*", Effect: "allow"})
	if err != nil {
		t.Fatal(err)
	}
	changedGrant, err := store.upsertGrant(ctx, grantRecord{UserID: viewer.ID, Action: "groups:manage", Scope: "stream:prod:*", Effect: "deny"})
	if err != nil || changedGrant.ID != grant.ID || changedGrant.Effect != "deny" {
		t.Fatalf("grant update=%+v original=%+v err=%v", changedGrant, grant, err)
	}
	editedGrant, err := store.updateGrant(ctx, grant.ID, grantRecord{UserID: viewer.ID, Action: "groups:read", Scope: "stream:prod:payments.*", Effect: "allow"})
	if err != nil || editedGrant.ID != grant.ID || editedGrant.Action != "groups:read" {
		t.Fatalf("grant edit=%+v original=%+v err=%v", editedGrant, grant, err)
	}

	store.writeAccessLog(ctx, accessLog{UserID: viewer.ID, Username: viewer.Username, Method: "GET", Path: "/api/streams", Action: "streams:read", Scope: "connection:prod", Status: 200, RequestID: "req-1"})
	logs, err := store.listAccessLogs(ctx, 10)
	if err != nil || len(logs) != 1 {
		t.Fatalf("access logs len=%d err=%v", len(logs), err)
	}
}

func TestStorePersistsMonitoredStreamsPerConnection(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	ctx := context.Background()

	item, created, err := store.addMonitoredStream(ctx, "primary", "orders:events", "admin")
	if err != nil || !created || item.ConnectionID != "primary" || item.Key != "orders:events" {
		t.Fatalf("add monitored stream: item=%+v created=%v err=%v", item, created, err)
	}
	duplicate, created, err := store.addMonitoredStream(ctx, "primary", "orders:events", "another-user")
	if err != nil || created || duplicate.CreatedBy != "admin" {
		t.Fatalf("duplicate monitored stream: item=%+v created=%v err=%v", duplicate, created, err)
	}
	if _, created, err := store.addMonitoredStream(ctx, "archive", "orders:events", "admin"); err != nil || !created {
		t.Fatalf("same key must be independently monitored by another connection: created=%v err=%v", created, err)
	}

	primary, err := store.listMonitoredStreams(ctx, "primary")
	if err != nil || len(primary) != 1 || primary[0].Key != "orders:events" {
		t.Fatalf("list primary monitored streams: items=%+v err=%v", primary, err)
	}
	deleted, err := store.deleteMonitoredStream(ctx, "primary", "orders:events")
	if err != nil || !deleted {
		t.Fatalf("delete monitored stream: deleted=%v err=%v", deleted, err)
	}
	deleted, err = store.deleteMonitoredStream(ctx, "primary", "orders:events")
	if err != nil || deleted {
		t.Fatalf("second delete must be idempotently absent: deleted=%v err=%v", deleted, err)
	}
	archive, err := store.listMonitoredStreams(ctx, "archive")
	if err != nil || len(archive) != 1 {
		t.Fatalf("deleting primary must not affect archive: items=%+v err=%v", archive, err)
	}
}

func TestInitialAdminCanOnlyBeCreatedOnce(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	hasUsers, err := store.hasUsers(context.Background())
	if err != nil || hasUsers {
		t.Fatalf("new store should be unconfigured: users=%v err=%v", hasUsers, err)
	}
	hash, _ := hashPassword("one-time-password-123")
	if _, err := store.createInitialAdmin(context.Background(), "admin", "Administrator", hash); err != nil {
		t.Fatal(err)
	}
	if _, err := store.createInitialAdmin(context.Background(), "attacker", "Attacker", hash); err == nil {
		t.Fatal("second initial administrator must be rejected")
	}
	hasUsers, err = store.hasUsers(context.Background())
	if err != nil || !hasUsers {
		t.Fatalf("store should be configured: users=%v err=%v", hasUsers, err)
	}
	reopened, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if _, _, err := reopened.authenticate(context.Background(), "admin"); err != nil {
		t.Fatalf("existing administrator was not preserved: %v", err)
	}
}

func TestEnsureDefaultAdminCreatesReusableCredentials(t *testing.T) {
	config := appConfig{DataPath: filepath.Join(t.TempDir(), "redisstreamscope.db"), SessionTTL: time.Hour}
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := ensureDefaultAdmin(store); err != nil {
		t.Fatal(err)
	}
	admin, passwordHash, err := store.authenticate(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	if admin.PasswordChangeRequired {
		t.Fatal("default administrator must not be forced to change the password")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte("password")); err != nil {
		t.Fatal("default administrator password does not match")
	}
	if err := ensureDefaultAdmin(store); err != nil {
		t.Fatal(err)
	}
	users, err := store.listUsers(context.Background())
	if err != nil || len(users) != 1 {
		t.Fatalf("default administrator must only be created once: users=%d err=%v", len(users), err)
	}
}
