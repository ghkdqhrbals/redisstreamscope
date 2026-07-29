package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type connectionConfig struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Mode         string   `json:"mode"`
	Addrs        []string `json:"addrs"`
	MasterName   string   `json:"masterName,omitempty"`
	Username     string   `json:"username,omitempty"`
	Password     string   `json:"password,omitempty"`
	PasswordEnv  string   `json:"passwordEnv,omitempty"`
	PasswordFile string   `json:"passwordFile,omitempty"`
	DB           int      `json:"db,omitempty"`
	TLS          bool     `json:"tls,omitempty"`
	TLSServer    string   `json:"tlsServerName,omitempty"`
	TLSCAFile    string   `json:"tlsCAFile,omitempty"`
	TLSCertFile  string   `json:"tlsCertFile,omitempty"`
	TLSKeyFile   string   `json:"tlsKeyFile,omitempty"`
	KeyPattern   string   `json:"keyPattern,omitempty"`
}

type propertiesConfig struct {
	Version int
	Server  struct {
		SecureCookies *bool
		SessionTTL    string
	}
	Redis struct {
		Connections []connectionConfig
	}
}

type appConfig struct {
	Addr           string
	DataPath       string
	ConfigPath     string
	SessionTTL     time.Duration
	SecureCookies  bool
	Connections    []connectionConfig
	MaxPageSize    int64
	MaxLiveStreams int
}

func loadConfig() (appConfig, error) {
	port, err := serverPort()
	if err != nil {
		return appConfig{}, err
	}
	cfg := appConfig{
		Addr:           ":" + port,
		DataPath:       envOr("DATA_PATH", "/data/streamscope.db"),
		ConfigPath:     envOr("CONFIG_PATH", "/data/config.properties"),
		SessionTTL:     12 * time.Hour,
		SecureCookies:  envBool("SECURE_COOKIES", false),
		MaxPageSize:    500,
		MaxLiveStreams: 8,
	}
	if raw := strings.TrimSpace(os.Getenv("SESSION_TTL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return cfg, fmt.Errorf("invalid SESSION_TTL: %w", err)
		}
		cfg.SessionTTL = parsed
	}
	document, exists, err := readPropertiesConfig(cfg.ConfigPath)
	if err != nil {
		return cfg, err
	}
	if exists {
		if document.Server.SecureCookies != nil {
			cfg.SecureCookies = *document.Server.SecureCookies
		}
		if document.Server.SessionTTL != "" {
			parsed, parseErr := time.ParseDuration(document.Server.SessionTTL)
			if parseErr != nil {
				return cfg, fmt.Errorf("invalid server.sessionTTL: %w", parseErr)
			}
			cfg.SessionTTL = parsed
		}
		cfg.Connections = document.Redis.Connections
	}
	redisEnvironmentConfigured, err := applyRedisEnvironment(&cfg)
	if err != nil {
		return cfg, err
	}
	if err := validateConnections(cfg.Connections); err != nil {
		return cfg, err
	}
	if redisEnvironmentConfigured {
		if err := savePropertiesConfig(cfg.ConfigPath, cfg); err != nil {
			return cfg, fmt.Errorf("persist Redis environment configuration: %w", err)
		}
	}
	return cfg, nil
}

func serverPort() (string, error) {
	port := envOr("PORT", "8080")
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", errors.New("PORT must be an integer from 1 to 65535")
	}
	return strconv.Itoa(number), nil
}

func applyRedisEnvironment(cfg *appConfig) (bool, error) {
	host, hostSet := lookupTrimmedEnvironment("REDIS_HOST")
	nodes, nodesSet := lookupTrimmedEnvironment("REDIS_NODES")
	redisURL, urlSet := lookupTrimmedEnvironment("REDIS_URL")
	if !hostSet && !nodesSet && !urlSet {
		return false, nil
	}
	if countTrue(hostSet, nodesSet, urlSet) > 1 {
		return false, errors.New("use only one of REDIS_HOST, REDIS_NODES, or REDIS_URL")
	}

	connectionID := "default"
	if len(cfg.Connections) == 1 {
		connectionID = cfg.Connections[0].ID
	}
	if value, present := lookupTrimmedEnvironment("REDIS_ID"); present {
		connectionID = value
	}

	index := -1
	for candidateIndex := range cfg.Connections {
		if cfg.Connections[candidateIndex].ID == connectionID {
			index = candidateIndex
			break
		}
	}
	connection := connectionConfig{
		ID: connectionID, Name: "Redis", Mode: "standalone", DB: 0, KeyPattern: "*",
	}
	if index >= 0 {
		connection = cfg.Connections[index]
	}

	port, err := redisPort()
	if err != nil {
		return false, err
	}
	switch {
	case hostSet:
		address, addressErr := redisAddress(host, port)
		if addressErr != nil {
			return false, addressErr
		}
		connection.Addrs = []string{address}
	case nodesSet:
		connection.Addrs = nil
		for _, node := range splitPropertyList(nodes) {
			address, addressErr := redisAddress(node, port)
			if addressErr != nil {
				return false, addressErr
			}
			connection.Addrs = append(connection.Addrs, address)
		}
	case urlSet:
		connection.Addrs = []string{redisURL}
	}

	if value, present := lookupTrimmedEnvironment("REDIS_NAME"); present {
		connection.Name = value
	}
	if value, present := lookupTrimmedEnvironment("REDIS_MODE"); present {
		connection.Mode = value
	} else if nodesSet && len(connection.Addrs) > 1 {
		connection.Mode = "cluster"
	}
	if value, present := lookupTrimmedEnvironment("REDIS_MASTER_NAME"); present {
		connection.MasterName = value
	}
	if value, present := lookupEnvironment("REDIS_USERNAME"); present {
		connection.Username = value
	}
	if value, present := lookupEnvironment("REDIS_PASSWORD"); present {
		connection.Password = value
		connection.PasswordEnv = ""
		connection.PasswordFile = ""
	}
	if value, present := lookupTrimmedEnvironment("REDIS_PASSWORD_FILE"); present {
		connection.Password = ""
		connection.PasswordEnv = ""
		connection.PasswordFile = value
	}
	if value, present := firstEnvironment("REDIS_DATABASE", "REDIS_DB"); present {
		database, parseErr := strconv.Atoi(strings.TrimSpace(value))
		if parseErr != nil || database < 0 {
			return false, errors.New("REDIS_DATABASE must be a non-negative integer")
		}
		connection.DB = database
	}
	if value, present := lookupEnvironment("REDIS_KEY_PATTERN"); present {
		connection.KeyPattern = value
	}
	if value, present := lookupTrimmedEnvironment("REDIS_TLS"); present {
		enabled, parseErr := strconv.ParseBool(value)
		if parseErr != nil {
			return false, errors.New("REDIS_TLS must be true or false")
		}
		connection.TLS = enabled
	}
	if value, present := lookupTrimmedEnvironment("REDIS_TLS_SERVER_NAME"); present {
		connection.TLSServer = value
	}
	if value, present := lookupTrimmedEnvironment("REDIS_TLS_CA_FILE"); present {
		connection.TLSCAFile = value
	}
	if value, present := lookupTrimmedEnvironment("REDIS_TLS_CERT_FILE"); present {
		connection.TLSCertFile = value
	}
	if value, present := lookupTrimmedEnvironment("REDIS_TLS_KEY_FILE"); present {
		connection.TLSKeyFile = value
	}

	if index >= 0 {
		cfg.Connections[index] = connection
	} else {
		cfg.Connections = append(cfg.Connections, connection)
	}
	return true, nil
}

func redisPort() (string, error) {
	port := envOr("REDIS_PORT", "6379")
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", errors.New("REDIS_PORT must be an integer from 1 to 65535")
	}
	return strconv.Itoa(number), nil
}

func redisAddress(host, port string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", errors.New("Redis host or node cannot be empty")
	}
	if strings.Contains(host, "://") {
		return host, nil
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host, nil
	}
	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	return net.JoinHostPort(host, port), nil
}

func lookupEnvironment(name string) (string, bool) {
	value, present := os.LookupEnv(name)
	return value, present
}

func lookupTrimmedEnvironment(name string) (string, bool) {
	value, present := lookupEnvironment(name)
	return strings.TrimSpace(value), present
}

func firstEnvironment(names ...string) (string, bool) {
	for _, name := range names {
		if value, present := lookupEnvironment(name); present {
			return value, true
		}
	}
	return "", false
}

func countTrue(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func readPropertiesConfig(path string) (propertiesConfig, bool, error) {
	var document propertiesConfig
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return document, false, nil
	}
	if err != nil {
		return document, false, fmt.Errorf("read CONFIG_PATH: %w", err)
	}
	values, err := parseProperties(string(data))
	if err != nil {
		return document, false, fmt.Errorf("parse CONFIG_PATH: %w", err)
	}
	if len(values) == 0 {
		return document, false, nil
	}
	if raw := values["version"]; raw != "" {
		document.Version, err = strconv.Atoi(raw)
		if err != nil {
			return document, false, errors.New("version must be an integer")
		}
	}
	if raw, present := values["server.secureCookies"]; present {
		parsed, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return document, false, errors.New("server.secureCookies must be true or false")
		}
		document.Server.SecureCookies = &parsed
	}
	document.Server.SessionTTL = values["server.sessionTTL"]
	count := 0
	if raw, present := values["redis.connections"]; present {
		count, err = strconv.Atoi(raw)
		if err != nil || count < 0 || count > 100 {
			return document, false, errors.New("redis.connections must be an integer from 0 to 100")
		}
	}
	document.Redis.Connections = make([]connectionConfig, 0, count)
	for index := 0; index < count; index++ {
		prefix := fmt.Sprintf("redis.%d.", index)
		database := 0
		if raw := values[prefix+"database"]; raw != "" {
			database, err = strconv.Atoi(raw)
			if err != nil || database < 0 {
				return document, false, fmt.Errorf("%sdatabase must be a non-negative integer", prefix)
			}
		}
		tlsEnabled := false
		if raw := values[prefix+"tls"]; raw != "" {
			tlsEnabled, err = strconv.ParseBool(raw)
			if err != nil {
				return document, false, fmt.Errorf("%stls must be true or false", prefix)
			}
		}
		document.Redis.Connections = append(document.Redis.Connections, connectionConfig{
			ID: values[prefix+"id"], Name: values[prefix+"name"], Mode: values[prefix+"mode"],
			Addrs: splitPropertyList(values[prefix+"addresses"]), MasterName: values[prefix+"masterName"],
			Username: values[prefix+"username"], Password: values[prefix+"password"],
			PasswordEnv: values[prefix+"passwordEnv"], PasswordFile: values[prefix+"passwordFile"],
			DB: database, KeyPattern: values[prefix+"keyPattern"], TLS: tlsEnabled,
			TLSServer: values[prefix+"tlsServerName"], TLSCAFile: values[prefix+"tlsCAFile"],
			TLSCertFile: values[prefix+"tlsCertFile"], TLSKeyFile: values[prefix+"tlsKeyFile"],
		})
	}
	return document, true, nil
}

func savePropertiesConfig(path string, cfg appConfig) error {
	data := []byte(renderProperties(cfg, false))
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".config-*.properties")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func renderProperties(cfg appConfig, maskPasswords bool) string {
	var builder strings.Builder
	builder.WriteString("# Managed by StreamScope. Changes made in Settings replace this file.\n")
	builder.WriteString("version=1\n")
	writeProperty(&builder, "server.secureCookies", strconv.FormatBool(cfg.SecureCookies))
	writeProperty(&builder, "server.sessionTTL", cfg.SessionTTL.String())
	writeProperty(&builder, "redis.connections", strconv.Itoa(len(cfg.Connections)))
	for index, connection := range cfg.Connections {
		prefix := fmt.Sprintf("redis.%d.", index)
		password := connection.Password
		if maskPasswords && password != "" {
			password = "********"
		}
		writeProperty(&builder, prefix+"id", connection.ID)
		writeProperty(&builder, prefix+"name", connection.Name)
		writeProperty(&builder, prefix+"mode", connection.Mode)
		writeProperty(&builder, prefix+"addresses", strings.Join(connection.Addrs, ","))
		writeProperty(&builder, prefix+"masterName", connection.MasterName)
		writeProperty(&builder, prefix+"username", connection.Username)
		writeProperty(&builder, prefix+"password", password)
		writeProperty(&builder, prefix+"passwordEnv", connection.PasswordEnv)
		writeProperty(&builder, prefix+"passwordFile", connection.PasswordFile)
		writeProperty(&builder, prefix+"database", strconv.Itoa(connection.DB))
		writeProperty(&builder, prefix+"keyPattern", connection.KeyPattern)
		writeProperty(&builder, prefix+"tls", strconv.FormatBool(connection.TLS))
		writeProperty(&builder, prefix+"tlsServerName", connection.TLSServer)
		writeProperty(&builder, prefix+"tlsCAFile", connection.TLSCAFile)
		writeProperty(&builder, prefix+"tlsCertFile", connection.TLSCertFile)
		writeProperty(&builder, prefix+"tlsKeyFile", connection.TLSKeyFile)
	}
	return builder.String()
}

func parseProperties(input string) (map[string]string, error) {
	result := make(map[string]string)
	for lineNumber, line := range strings.Split(input, "\n") {
		line = strings.TrimSuffix(line, "\r")
		trimmed := strings.TrimLeft(line, " \t\f")
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "!") {
			continue
		}
		delimiter := propertyDelimiter(trimmed)
		if delimiter < 0 {
			return nil, fmt.Errorf("line %d must use key=value", lineNumber+1)
		}
		key, err := unescapeProperty(strings.TrimSpace(trimmed[:delimiter]))
		if err != nil || key == "" {
			return nil, fmt.Errorf("line %d has an invalid key", lineNumber+1)
		}
		rawValue := strings.TrimLeft(trimmed[delimiter+1:], " \t\f")
		value, err := unescapeProperty(rawValue)
		if err != nil {
			return nil, fmt.Errorf("line %d has an invalid value: %w", lineNumber+1, err)
		}
		result[key] = value
	}
	return result, nil
}

func propertyDelimiter(value string) int {
	escaped := false
	for index, character := range value {
		if escaped {
			escaped = false
			continue
		}
		if character == '\\' {
			escaped = true
			continue
		}
		if character == '=' || character == ':' {
			return index
		}
	}
	return -1
}

func unescapeProperty(value string) (string, error) {
	var builder strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' {
			builder.WriteByte(value[index])
			continue
		}
		index++
		if index >= len(value) {
			return "", errors.New("trailing escape")
		}
		switch value[index] {
		case 'n':
			builder.WriteByte('\n')
		case 'r':
			builder.WriteByte('\r')
		case 't':
			builder.WriteByte('\t')
		case 'f':
			builder.WriteByte('\f')
		case 'u':
			if index+4 >= len(value) {
				return "", errors.New("incomplete unicode escape")
			}
			code, err := strconv.ParseUint(value[index+1:index+5], 16, 16)
			if err != nil {
				return "", errors.New("invalid unicode escape")
			}
			builder.WriteRune(rune(code))
			index += 4
		default:
			builder.WriteByte(value[index])
		}
	}
	return builder.String(), nil
}

func writeProperty(builder *strings.Builder, key, value string) {
	builder.WriteString(key)
	builder.WriteByte('=')
	builder.WriteString(escapeProperty(value))
	builder.WriteByte('\n')
}

func escapeProperty(value string) string {
	var builder strings.Builder
	for index, character := range value {
		switch character {
		case '\\':
			builder.WriteString(`\\`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case '\f':
			builder.WriteString(`\f`)
		case ' ':
			if index == 0 || index == len(value)-1 {
				builder.WriteByte('\\')
			}
			builder.WriteRune(character)
		default:
			builder.WriteRune(character)
		}
	}
	return builder.String()
}

func splitPropertyList(value string) []string {
	if value == "" {
		return nil
	}
	return compactStrings(strings.Split(value, ","))
}

func validateConnections(connections []connectionConfig) error {
	seen := make(map[string]struct{}, len(connections))
	for index := range connections {
		connection := &connections[index]
		connection.ID = strings.TrimSpace(connection.ID)
		connection.Name = strings.TrimSpace(connection.Name)
		connection.Mode = strings.ToLower(strings.TrimSpace(connection.Mode))
		connection.Addrs = compactStrings(connection.Addrs)
		for addressIndex, address := range connection.Addrs {
			if !strings.Contains(address, "://") {
				continue
			}
			parsed, err := url.Parse(address)
			if err != nil || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") || parsed.Host == "" {
				return fmt.Errorf("connection %q has invalid Redis URL %q", connection.ID, address)
			}
			connection.Addrs[addressIndex] = parsed.Host
			if parsed.User != nil {
				if connection.Username == "" {
					connection.Username = parsed.User.Username()
				}
				if password, present := parsed.User.Password(); present && connection.Password == "" {
					connection.Password = password
				}
			}
			if parsed.Scheme == "rediss" {
				connection.TLS = true
			}
			if connection.Mode != "cluster" && parsed.Path != "" && parsed.Path != "/" {
				database, parseErr := strconv.Atoi(strings.TrimPrefix(parsed.Path, "/"))
				if parseErr != nil || database < 0 {
					return fmt.Errorf("connection %q has invalid database in Redis URL", connection.ID)
				}
				connection.DB = database
			}
		}
		if connection.ID == "" || len(connection.Addrs) == 0 {
			return errors.New("each Redis connection needs id and at least one address")
		}
		if _, exists := seen[connection.ID]; exists {
			return fmt.Errorf("duplicate Redis connection id %q", connection.ID)
		}
		seen[connection.ID] = struct{}{}
		if connection.Name == "" {
			connection.Name = connection.ID
		}
		if connection.Mode == "" {
			connection.Mode = "standalone"
		}
		if connection.KeyPattern == "" {
			connection.KeyPattern = "*"
		}
		switch connection.Mode {
		case "standalone", "sentinel", "cluster":
		default:
			return fmt.Errorf("unsupported Redis mode %q", connection.Mode)
		}
		if connection.Mode == "sentinel" && strings.TrimSpace(connection.MasterName) == "" {
			return fmt.Errorf("sentinel connection %q needs masterName", connection.ID)
		}
		if connection.Mode == "cluster" && connection.DB != 0 {
			return fmt.Errorf("cluster connection %q must use database 0", connection.ID)
		}
		if (connection.TLSCertFile == "") != (connection.TLSKeyFile == "") {
			return fmt.Errorf("connection %q needs both tlsCertFile and tlsKeyFile", connection.ID)
		}
	}
	return nil
}

func (c connectionConfig) password() (string, error) {
	if c.PasswordFile != "" {
		return readSecretFile(c.PasswordFile)
	}
	if c.PasswordEnv != "" {
		return os.Getenv(c.PasswordEnv), nil
	}
	return c.Password, nil
}

func (c connectionConfig) tlsConfig() (*tls.Config, error) {
	if !c.TLS {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TLSServer}
	if c.TLSCAFile != "" {
		data, err := os.ReadFile(c.TLSCAFile)
		if err != nil {
			return nil, err
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(data) {
			return nil, errors.New("TLS CA file contains no certificates")
		}
		config.RootCAs = pool
	}
	if c.TLSCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func compactStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func readSecretFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimRight(string(data), "\r\n")
	if value == "" {
		return "", errors.New("secret file is empty")
	}
	return value, nil
}
