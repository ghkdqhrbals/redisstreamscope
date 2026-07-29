package main

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestProjectVersionFilesMatch(t *testing.T) {
	versionBytes, err := os.ReadFile("VERSION")
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}
	version := strings.TrimSpace(string(versionBytes))
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(version) {
		t.Fatalf("VERSION %q is not a semantic version", version)
	}

	for _, path := range []string{"package.json", "package-lock.json"} {
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("read %s: %v", path, readErr)
		}
		var document struct {
			Version  string `json:"version"`
			Packages map[string]struct {
				Version string `json:"version"`
			} `json:"packages"`
		}
		if decodeErr := json.Unmarshal(content, &document); decodeErr != nil {
			t.Fatalf("decode %s: %v", path, decodeErr)
		}
		if document.Version != version {
			t.Fatalf("%s version %q does not match VERSION %q", path, document.Version, version)
		}
		if rootPackage, exists := document.Packages[""]; exists && rootPackage.Version != version {
			t.Fatalf("%s root package version %q does not match VERSION %q", path, rootPackage.Version, version)
		}
	}
}
