package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//go:embed all:dist/web
var webAssets embed.FS

var buildVersion = "dev"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		port, err := serverPort()
		if err != nil {
			os.Exit(1)
		}
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get("http://127.0.0.1:" + port + "/health/live")
		if err != nil || response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		_ = response.Body.Close()
		return
	}
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	if len(os.Args) == 2 && os.Args[1] == "print-config" {
		_, _ = fmt.Print(renderProperties(config, true))
		return
	}
	store, err := openStore(config)
	if err != nil {
		log.Fatalf("open StreamScope database: %v", err)
	}
	defer store.close()
	if err := ensureDefaultAdmin(store); err != nil {
		log.Fatalf("create default administrator: %v", err)
	}
	redisManager, err := newRedisManager(config)
	if err != nil {
		log.Fatalf("configure Redis: %v", err)
	}
	defer redisManager.close()

	server, err := newAPIServer(config, store, redisManager, webAssets)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}
	httpServer := &http.Server{
		Addr:              config.Addr,
		Handler:           server,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}

	go func() {
		log.Printf(`{"level":"info","message":"StreamScope started","addr":%q}`, config.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)
}

func ensureDefaultAdmin(store *store) error {
	configured, err := store.hasUsers(context.Background())
	if err != nil {
		return err
	}
	if configured {
		return nil
	}
	passwordHash, err := hashPassword("password")
	if err != nil {
		return err
	}
	_, err = store.createInitialAdmin(context.Background(), "admin", "System Administrator", passwordHash)
	if err == nil {
		log.Print(`{"level":"info","message":"Default administrator created","username":"admin"}`)
	}
	return err
}

func serveSPA(assets embed.FS) (http.Handler, error) {
	root, err := fs.Sub(assets, "dist/web")
	if err != nil {
		return nil, err
	}
	index, err := fs.ReadFile(root, "index.html")
	if err != nil {
		return nil, err
	}
	files := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			path := request.URL.Path[1:]
			if _, err := fs.Stat(root, path); err == nil {
				if path != "index.html" {
					writer.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				files.ServeHTTP(writer, request)
				return
			}
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		writer.Header().Set("Cache-Control", "no-store")
		_, _ = writer.Write(index)
	}), nil
}

func mustJSON(value any) []byte {
	result, _ := json.Marshal(value)
	return result
}
