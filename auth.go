package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookieName       = "redisstreamscope_session"
	legacySessionCookieName = "streamscope_session"
)

type loginWindow struct {
	Attempts int
	ResetAt  time.Time
}

type authenticator struct {
	config   appConfig
	store    *store
	mu       sync.Mutex
	attempts map[string]loginWindow
}

func newAuthenticator(config appConfig, store *store) *authenticator {
	return &authenticator{config: config, store: store, attempts: make(map[string]loginWindow)}
}

func (a *authenticator) login(ctx context.Context, username, password, ip, userAgent string) (userRecord, string, time.Time, error) {
	user, passwordHash, err := a.store.authenticate(ctx, username)
	if err != nil || !user.Enabled || bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(password)) != nil {
		return userRecord{}, "", time.Time{}, errors.New("invalid username or password")
	}
	token, expires, err := a.store.createSession(ctx, user, a.config.SessionTTL, ip, userAgent)
	return user, token, expires, err
}

func (a *authenticator) session(request *http.Request) (sessionRecord, error) {
	cookie, err := authenticationCookie(request)
	if err != nil {
		return sessionRecord{}, err
	}
	return a.store.session(request.Context(), cookie.Value)
}

func (a *authenticator) logout(request *http.Request) {
	seen := make(map[string]struct{}, 2)
	for _, name := range []string{sessionCookieName, legacySessionCookieName} {
		if cookie, err := request.Cookie(name); err == nil {
			if _, exists := seen[cookie.Value]; exists {
				continue
			}
			seen[cookie.Value] = struct{}{}
			a.store.deleteSession(request.Context(), cookie.Value)
		}
	}
}

func (a *authenticator) setCookie(writer http.ResponseWriter, token string, expires time.Time) {
	http.SetCookie(writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expires,
		MaxAge:   int(time.Until(expires).Seconds()),
		HttpOnly: true,
		Secure:   a.config.SecureCookies,
		SameSite: http.SameSiteStrictMode,
	})
}

func (a *authenticator) clearCookie(writer http.ResponseWriter) {
	for _, name := range []string{sessionCookieName, legacySessionCookieName} {
		http.SetCookie(writer, &http.Cookie{
			Name: name, Value: "", Path: "/", MaxAge: -1,
			HttpOnly: true, Secure: a.config.SecureCookies, SameSite: http.SameSiteStrictMode,
		})
	}
}

func authenticationCookie(request *http.Request) (*http.Cookie, error) {
	cookie, err := request.Cookie(sessionCookieName)
	if err == nil {
		return cookie, nil
	}
	if !errors.Is(err, http.ErrNoCookie) {
		return nil, err
	}
	return request.Cookie(legacySessionCookieName)
}

func (a *authenticator) allowLogin(request *http.Request) bool {
	host := requestIP(request)
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	window := a.attempts[host]
	if now.After(window.ResetAt) {
		window = loginWindow{ResetAt: now.Add(10 * time.Minute)}
	}
	if window.Attempts >= 5 {
		a.attempts[host] = window
		return false
	}
	window.Attempts++
	a.attempts[host] = window
	if len(a.attempts) > 512 {
		for key, item := range a.attempts {
			if now.After(item.ResetAt) {
				delete(a.attempts, key)
			}
		}
	}
	return true
}

func (a *authenticator) resetAttempts(request *http.Request) {
	host := requestIP(request)
	a.mu.Lock()
	delete(a.attempts, host)
	a.mu.Unlock()
}

func requestIP(request *http.Request) string {
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil {
		return request.RemoteAddr
	}
	return host
}
