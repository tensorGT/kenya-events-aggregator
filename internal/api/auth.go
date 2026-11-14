package api

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cladder/ke-events/internal/cache"
	"github.com/cladder/ke-events/internal/telemetry"
	"github.com/golang-jwt/jwt/v5"
)

type ctxKey string

const claimsKey ctxKey = "jwt_claims"

type Claims struct {
	Sub  string `json:"sub"`
	Role string `json:"role"` // reader | contributor | admin
	jwt.RegisteredClaims
}

func IssueJWT(secret, sub, role string) (string, error) {
	c := Claims{
		Sub:  sub,
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString([]byte(secret))
}

func RequireRole(secret string, allowedRoles ...string) func(http.Handler) http.Handler {
	allowed := make(map[string]bool)
	for _, r := range allowedRoles {
		allowed[r] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing bearer token"})
				return
			}
			tokStr := strings.TrimPrefix(auth, "Bearer ")
			tok, err := jwt.ParseWithClaims(tokStr, &Claims{}, func(t *jwt.Token) (any, error) {
				return []byte(secret), nil
			})
			if err != nil || !tok.Valid {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token"})
				return
			}
			claims, ok := tok.Claims.(*Claims)
			if !ok || !allowed[claims.Role] {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "insufficient role"})
				return
			}
			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func GetClaims(ctx context.Context) *Claims {
	if c, ok := ctx.Value(claimsKey).(*Claims); ok {
		return c
	}
	return &Claims{Sub: "anonymous", Role: "reader"}
}

func RateLimitMiddleware(rdb *cache.Client, limit int, window time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			if xff := r.Header.Get("X-Real-IP"); xff != "" {
				ip = xff
			}
			ok, _, err := rdb.AllowSlidingWindow(r.Context(), ip, limit, window)
			if err == nil && !ok {
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(ww, r)
		dur := time.Since(start).Seconds()
		telemetry.HTTPRequestsTotal.WithLabelValues(r.Method, r.URL.Path, http.StatusText(ww.status)).Inc()
		telemetry.HTTPRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(dur)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijacker not supported")
	}
	return h.Hijack()
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
