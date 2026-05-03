package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gama/queuescope/internal/auth"
	"github.com/gama/queuescope/internal/config"
	"github.com/gama/queuescope/internal/domain"
	"github.com/gama/queuescope/internal/providers"
	"github.com/gama/queuescope/internal/providers/bullmq"
	"github.com/gama/queuescope/internal/providers/rabbitmq"
	"github.com/gama/queuescope/internal/providers/sqs"
	"github.com/gama/queuescope/internal/store"
	"github.com/gin-gonic/gin"
)

const userContextKey = "user"

type Server struct {
	cfg      config.Config
	auth     *auth.Manager
	registry *providers.Registry
	store    *store.ConnectionStore
	admin    domain.User
}

func NewServer(cfg config.Config, connectionStore *store.ConnectionStore) *Server {
	return &Server{
		cfg:  cfg,
		auth: auth.NewManager(cfg.SessionSecret),
		registry: providers.NewRegistry(
			bullmq.New(),
			sqs.New(),
			rabbitmq.New(),
		),
		store: connectionStore,
		admin: domain.User{
			ID:    "local-admin",
			Email: cfg.AdminEmail,
			Role:  domain.RoleAdmin,
		},
	}
}

func (s *Server) Routes() *gin.Engine {
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery(), s.cors())

	api := router.Group("/api")
	api.GET("/health", s.handleHealth)
	api.POST("/auth/login", s.handleLogin)

	protected := api.Group("")
	protected.Use(s.requireAuth())
	protected.POST("/auth/logout", s.handleLogout)
	protected.GET("/auth/me", s.handleMe)
	protected.GET("/providers", s.handleProviders)
	protected.GET("/connections", s.handleListConnections)
	protected.POST("/connections", s.handleCreateConnection)
	protected.GET("/connections/:connectionId/health", s.handleConnectionHealth)
	protected.DELETE("/connections/:connectionId", s.handleDeleteConnection)
	protected.GET("/connections/:connectionId/queues", s.handleListQueues)
	protected.GET("/connections/:connectionId/queues/:queueName/messages", s.handleListMessages)
	protected.GET("/connections/:connectionId/queues/:queueName/messages/:messageId", s.handleGetMessage)
	protected.POST("/connections/:connectionId/queues/:queueName/messages/:messageId/retry", s.handleRetryMessage)
	protected.DELETE("/connections/:connectionId/queues/:queueName/messages/:messageId", s.handleDeleteMessage)
	protected.GET("/audit-log", s.handleAuditLog)

	return router
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"time":   time.Now().UTC(),
	})
}

func (s *Server) handleLogin(c *gin.Context) {
	var payload struct {
		Email    string `json:"email" binding:"required,email"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid login payload"})
		return
	}

	if !strings.EqualFold(payload.Email, s.cfg.AdminEmail) || payload.Password != s.cfg.AdminPassword {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid email or password"})
		return
	}

	token, session, err := s.auth.Create(s.admin)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create session"})
		return
	}

	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(auth.CookieName, token, int(time.Until(session.ExpiresAt).Seconds()), "/", "", false, true)
	c.JSON(http.StatusOK, gin.H{
		"user":      session.User,
		"expiresAt": session.ExpiresAt,
	})
}

func (s *Server) handleLogout(c *gin.Context) {
	token, err := c.Cookie(auth.CookieName)
	if err == nil {
		s.auth.Delete(token)
	}

	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(auth.CookieName, "", -1, "/", "", false, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *Server) handleMe(c *gin.Context) {
	user, ok := userFromContext(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"user": user})
}

func (s *Server) handleProviders(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"providers": s.registry.Providers(),
	})
}

func (s *Server) handleListConnections(c *gin.Context) {
	connections, err := s.store.List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list connections"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"connections": connections,
	})
}

func (s *Server) handleCreateConnection(c *gin.Context) {
	var payload struct {
		Name     string                 `json:"name" binding:"required"`
		Provider domain.QueueProvider   `json:"provider" binding:"required"`
		Mode     domain.ConnectionMode  `json:"mode"`
		Config   map[string]interface{} `json:"config"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid connection payload"})
		return
	}

	if _, ok := s.registry.Get(payload.Provider); !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported provider"})
		return
	}

	mode := payload.Mode
	if mode == "" {
		mode = domain.ConnectionReadOnly
	}
	if mode != domain.ConnectionReadOnly && mode != domain.ConnectionOperator {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid connection mode"})
		return
	}

	if payload.Config == nil {
		payload.Config = map[string]interface{}{}
	}

	connection, err := s.store.Create(c.Request.Context(), domain.QueueConnection{
		Name:     strings.TrimSpace(payload.Name),
		Provider: payload.Provider,
		Mode:     mode,
		Config:   payload.Config,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create connection"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"connection": connection})
}

func (s *Server) handleConnectionHealth(c *gin.Context) {
	connectionID := c.Param("connectionId")
	connection, err := s.store.Get(c.Request.Context(), connectionID)
	if err != nil {
		if errors.Is(err, store.ErrConnectionNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "connection not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load connection"})
		return
	}

	adapter, ok := s.registry.Get(connection.Provider)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported provider"})
		return
	}

	if err := adapter.Test(c.Request.Context(), connection); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

func (s *Server) handleDeleteConnection(c *gin.Context) {
	connectionID := c.Param("connectionId")
	if err := s.store.Delete(c.Request.Context(), connectionID); err != nil {
		if errors.Is(err, store.ErrConnectionNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "connection not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete connection"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *Server) handleListQueues(c *gin.Context) {
	connection, adapter, ok := s.connectionAdapter(c)
	if !ok {
		return
	}

	queues, err := adapter.ListQueues(c.Request.Context(), connection)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list queues"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"queues": queues})
}

func (s *Server) handleListMessages(c *gin.Context) {
	connection, adapter, ok := s.connectionAdapter(c)
	if !ok {
		return
	}

	limit := 50
	var filter providers.MessageFilter
	filter.Status = domain.MessageStatus(c.Query("status"))
	filter.Query = c.Query("query")
	filter.Limit = limit

	messages, err := adapter.ListMessages(c.Request.Context(), connection, c.Param("queueName"), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list messages"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"messages": messages})
}

func (s *Server) handleGetMessage(c *gin.Context) {
	connection, adapter, ok := s.connectionAdapter(c)
	if !ok {
		return
	}

	message, err := adapter.GetMessage(c.Request.Context(), connection, c.Param("queueName"), c.Param("messageId"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "message not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": message})
}

func (s *Server) handleRetryMessage(c *gin.Context) {
	s.handleMessageAction(c, domain.AuditRetryMessage, func(ctxConnection domain.QueueConnection, adapter providers.Adapter) error {
		return adapter.RetryMessage(c.Request.Context(), ctxConnection, c.Param("queueName"), c.Param("messageId"))
	})
}

func (s *Server) handleDeleteMessage(c *gin.Context) {
	s.handleMessageAction(c, domain.AuditDeleteMessage, func(ctxConnection domain.QueueConnection, adapter providers.Adapter) error {
		return adapter.DeleteMessage(c.Request.Context(), ctxConnection, c.Param("queueName"), c.Param("messageId"))
	})
}

func (s *Server) handleAuditLog(c *gin.Context) {
	entries, err := s.store.ListAuditEntries(c.Request.Context(), 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list audit log"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"entries": entries})
}

func (s *Server) handleMessageAction(c *gin.Context, action domain.AuditAction, run func(domain.QueueConnection, providers.Adapter) error) {
	connection, adapter, ok := s.connectionAdapter(c)
	if !ok {
		return
	}

	if connection.Mode != domain.ConnectionOperator {
		c.JSON(http.StatusForbidden, gin.H{"error": "connection is read-only"})
		return
	}

	user, _ := userFromContext(c)
	result := domain.AuditResultSuccess
	errorMessage := ""

	err := run(connection, adapter)
	if err != nil {
		result = domain.AuditResultFailure
		errorMessage = err.Error()
	}

	_, auditErr := s.store.CreateAuditEntry(c.Request.Context(), domain.AuditLogEntry{
		ActorID:      user.ID,
		ActorEmail:   user.Email,
		Action:       action,
		Result:       result,
		Provider:     connection.Provider,
		ConnectionID: connection.ID,
		QueueName:    c.Param("queueName"),
		MessageID:    c.Param("messageId"),
		Error:        errorMessage,
	})
	if auditErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "action ran but audit logging failed"})
		return
	}

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errorMessage})
		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (s *Server) connectionAdapter(c *gin.Context) (domain.QueueConnection, providers.Adapter, bool) {
	connectionID := c.Param("connectionId")
	connection, err := s.store.Get(c.Request.Context(), connectionID)
	if err != nil {
		if errors.Is(err, store.ErrConnectionNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "connection not found"})
			return domain.QueueConnection{}, nil, false
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load connection"})
		return domain.QueueConnection{}, nil, false
	}

	adapter, ok := s.registry.Get(connection.Provider)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported provider"})
		return domain.QueueConnection{}, nil, false
	}

	return connection, adapter, true
}

func (s *Server) requireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		token, err := c.Cookie(auth.CookieName)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}

		session, ok := s.auth.Get(token)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}

		c.Set(userContextKey, session.User)
		c.Next()
	}
}

func (s *Server) cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", s.cfg.AllowedOrigin)
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		c.Header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func userFromContext(c *gin.Context) (domain.User, bool) {
	value, ok := c.Get(userContextKey)
	if !ok {
		return domain.User{}, false
	}

	user, ok := value.(domain.User)
	return user, ok
}
