package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticationCookieUsesCurrentNameFirst(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.AddCookie(&http.Cookie{Name: legacySessionCookieName, Value: "legacy-token"})
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "current-token"})

	cookie, err := authenticationCookie(request)
	if err != nil {
		t.Fatal(err)
	}
	if cookie.Value != "current-token" {
		t.Fatalf("cookie = %q, want current-token", cookie.Value)
	}
}

func TestAuthenticationCookieAcceptsLegacyName(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.AddCookie(&http.Cookie{Name: legacySessionCookieName, Value: "legacy-token"})

	cookie, err := authenticationCookie(request)
	if err != nil {
		t.Fatal(err)
	}
	if cookie.Value != "legacy-token" {
		t.Fatalf("cookie = %q, want legacy-token", cookie.Value)
	}
}

func TestClearCookieExpiresCurrentAndLegacyNames(t *testing.T) {
	authenticator := &authenticator{config: appConfig{}}
	response := httptest.NewRecorder()
	authenticator.clearCookie(response)

	cookies := response.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("cookies = %d, want 2", len(cookies))
	}
	if cookies[0].Name != sessionCookieName || cookies[1].Name != legacySessionCookieName {
		t.Fatalf("unexpected cookie names: %q, %q", cookies[0].Name, cookies[1].Name)
	}
	for _, cookie := range cookies {
		if cookie.MaxAge != -1 {
			t.Fatalf("%s MaxAge = %d, want -1", cookie.Name, cookie.MaxAge)
		}
	}
}
