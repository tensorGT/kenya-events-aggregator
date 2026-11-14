package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRBACMiddleware(t *testing.T) {
	secret := "test-secret"
	protected := RequireRole(secret, "contributor", "admin")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Missing token -> 401
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources", nil)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	// 2. Reader role -> 403
	readerTok, _ := IssueJWT(secret, "u1", "reader")
	req = httptest.NewRequest(http.MethodPost, "/api/v1/sources", nil)
	req.Header.Set("Authorization", "Bearer "+readerTok)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}

	// 3. Admin role -> 200
	adminTok, _ := IssueJWT(secret, "u2", "admin")
	req = httptest.NewRequest(http.MethodPost, "/api/v1/sources", nil)
	req.Header.Set("Authorization", "Bearer "+adminTok)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestOpenAPIEndpoint(t *testing.T) {
	srv := &HTTPServer{}
	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()
	srv.handleOpenAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
