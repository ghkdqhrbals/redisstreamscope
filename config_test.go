package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommentOnlyConfigIsUnconfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.properties")
	if err := os.WriteFile(path, []byte("# version=1\n# redis.connections=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	document, exists, err := readPropertiesConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("comment-only configuration must be treated as unconfigured")
	}
	if len(document.Redis.Connections) != 0 {
		t.Fatal("comment-only configuration must have no connections")
	}
}

func TestSaveAndLoadPropertiesConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.properties")
	config := appConfig{
		ConfigPath:    path,
		SessionTTL:    18 * time.Hour,
		SecureCookies: true,
		Connections: []connectionConfig{{
			ID: "prod", Name: "Production", Mode: "standalone",
			Addrs: []string{"redis.internal:6379"}, Username: "redisstreamscope",
			Password: `not=returned:\to-browser `, DB: 2, KeyPattern: "orders.*",
		}},
	}
	if err := savePropertiesConfig(path, config); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config permission = %o, want 600", info.Mode().Perm())
	}
	document, exists, err := readPropertiesConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || document.Version != 1 {
		t.Fatal("saved configuration was not loaded")
	}
	if document.Server.SessionTTL != "18h0m0s" || document.Server.SecureCookies == nil || !*document.Server.SecureCookies {
		t.Fatal("server settings were not preserved")
	}
	if got := document.Redis.Connections[0]; got.ID != "prod" || got.Password != `not=returned:\to-browser ` || got.DB != 2 {
		t.Fatalf("unexpected Redis configuration: %#v", got)
	}
	masked := renderProperties(config, true)
	if strings.Contains(masked, `not=returned`) || !strings.Contains(masked, "redis.0.password=********") {
		t.Fatal("printable configuration did not mask the direct password")
	}
}

func TestRedisURLIsNormalized(t *testing.T) {
	connections := []connectionConfig{{
		ID: "secure", Name: "Secure", Mode: "standalone",
		Addrs: []string{"rediss://acl-user:secret@redis.internal:6380/3"},
	}}
	if err := validateConnections(connections); err != nil {
		t.Fatal(err)
	}
	connection := connections[0]
	if connection.Addrs[0] != "redis.internal:6380" {
		t.Fatalf("address = %q", connection.Addrs[0])
	}
	if connection.Username != "acl-user" || connection.Password != "secret" {
		t.Fatal("URL credentials were not extracted")
	}
	if !connection.TLS || connection.DB != 3 {
		t.Fatal("rediss URL TLS or database was not applied")
	}
	public := publicConnectionConfig(connection)
	if public["passwordConfigured"] != true {
		t.Fatal("public configuration must report that a password exists")
	}
	if got := public["addrs"].([]string)[0]; got != "redis.internal:6380" {
		t.Fatalf("public address leaked URL credentials: %q", got)
	}
	if _, present := public["password"]; present {
		t.Fatal("public configuration must not contain a password")
	}
}

func TestClusterRejectsNonZeroDatabase(t *testing.T) {
	connections := []connectionConfig{{
		ID: "cluster", Mode: "cluster", Addrs: []string{"redis-1:6379"}, DB: 1,
	}}
	if err := validateConnections(connections); err == nil {
		t.Fatal("cluster database other than zero must be rejected")
	}
}

func TestLoadConfigPersistsRedisEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.properties")
	t.Setenv("CONFIG_PATH", path)
	t.Setenv("DATA_PATH", filepath.Join(t.TempDir(), "redisstreamscope.db"))
	t.Setenv("PORT", "9090")
	t.Setenv("REDIS_HOST", "redis.internal")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("REDIS_ID", "production")
	t.Setenv("REDIS_NAME", "Production Redis")
	t.Setenv("REDIS_USERNAME", "redisstreamscope")
	t.Setenv("REDIS_PASSWORD", "secret password")
	t.Setenv("REDIS_DATABASE", "2")
	t.Setenv("REDIS_KEY_PATTERN", "orders:*")

	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.Addr != ":9090" {
		t.Fatalf("address = %q, want :9090", config.Addr)
	}
	if len(config.Connections) != 1 {
		t.Fatalf("connections = %d, want 1", len(config.Connections))
	}
	connection := config.Connections[0]
	if connection.ID != "production" || connection.Addrs[0] != "redis.internal:6380" {
		t.Fatalf("unexpected Redis connection: %#v", connection)
	}
	if connection.Password != "secret password" || connection.DB != 2 || connection.KeyPattern != "orders:*" {
		t.Fatalf("Redis settings were not imported: %#v", connection)
	}

	if err := os.Unsetenv("REDIS_HOST"); err != nil {
		t.Fatal(err)
	}
	persisted, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted.Connections) != 1 || persisted.Connections[0].Password != "secret password" {
		t.Fatal("Redis environment configuration was not loaded from the volume configuration")
	}
}

func TestLoadConfigRejectsInvalidPort(t *testing.T) {
	t.Setenv("PORT", "70000")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "PORT") {
		t.Fatalf("expected invalid PORT error, got %v", err)
	}
}
