package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/gama/queuescope/internal/domain"
)

const CookieName = "queuescope_session"

type Session struct {
	ID        string
	User      domain.User
	ExpiresAt time.Time
}

type Manager struct {
	secret   []byte
	mu       sync.RWMutex
	sessions map[string]Session
}

func NewManager(secret string) *Manager {
	return &Manager{
		secret:   []byte(secret),
		sessions: map[string]Session{},
	}
}

func (m *Manager) Create(user domain.User) (string, Session, error) {
	id, err := randomID()
	if err != nil {
		return "", Session{}, err
	}

	session := Session{
		ID:        id,
		User:      user,
		ExpiresAt: time.Now().Add(12 * time.Hour),
	}

	m.mu.Lock()
	m.sessions[id] = session
	m.mu.Unlock()

	return m.sign(id), session, nil
}

func (m *Manager) Get(token string) (Session, bool) {
	id, ok := m.verify(token)
	if !ok {
		return Session{}, false
	}

	m.mu.RLock()
	session, ok := m.sessions[id]
	m.mu.RUnlock()
	if !ok || time.Now().After(session.ExpiresAt) {
		return Session{}, false
	}

	return session, true
}

func (m *Manager) Delete(token string) {
	id, ok := m.verify(token)
	if !ok {
		return
	}

	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

func (m *Manager) sign(id string) string {
	mac := hmac.New(sha256.New, m.secret)
	mac.Write([]byte(id))
	signature := mac.Sum(nil)
	return id + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func (m *Manager) verify(token string) (string, bool) {
	id, signature, ok := strings.Cut(token, ".")
	if !ok || id == "" || signature == "" {
		return "", false
	}

	expected := m.sign(id)
	_, expectedSignature, ok := strings.Cut(expected, ".")
	if !ok {
		return "", false
	}

	return id, hmac.Equal([]byte(signature), []byte(expectedSignature))
}

func randomID() (string, error) {
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", errors.New("could not create session id")
	}
	return base64.RawURLEncoding.EncodeToString(bytes[:]), nil
}
