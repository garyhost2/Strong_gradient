package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"company-api/middleware" // Replace 'your-project' with your actual module name
)

// CompanyRequest represents the incoming request structure
type CompanyRequest struct {
	Companies []middleware.Company `json:"companies"`
}

// APIResponse represents the standard API response
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Server represents the API server
type Server struct {
	batchProcessor *middleware.BatchProcessor
	router        *mux.Router
	healthy       atomic.Bool
}

// NewServer creates a new API server instance
func NewServer(bp *middleware.BatchProcessor) *Server {
	s := &Server{
		batchProcessor: bp,
		router:        mux.NewRouter(),
	}
	s.healthy.Store(true)
	s.setupRoutes()
	return s
}

// setupRoutes configures the API endpoints
func (s *Server) setupRoutes() {
	// Create a subrouter for API v1
	api := s.router.PathPrefix("/api/v1").Subrouter()
	
	// Health check endpoint
	s.router.HandleFunc("/health", s.healthCheckHandler).Methods(http.MethodGet)
	
	// API endpoints
	api.HandleFunc("/companies/batch", s.batchUploadHandler).Methods(http.MethodPost)
	api.HandleFunc("/companies", s.fetchAllCompaniesHandler).Methods(http.MethodGet)
	api.HandleFunc("/companies/update-treated", s.updateTreatedHandler).Methods(http.MethodPut)
	
	// Apply middleware
	s.router.Use(s.loggingMiddleware)
}

// fetchAllCompaniesHandler fetches all companies
func (s *Server) fetchAllCompaniesHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	companies, err := s.batchProcessor.FetchAllCompanies(ctx)
	if err != nil {
		s.sendResponse(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: "Failed to fetch companies: " + err.Error(),
		})
		return
	}

	s.sendResponse(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Companies fetched successfully",
		Data:    companies,
	})
}

// healthCheckHandler performs a health check
func (s *Server) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := s.batchProcessor.HealthCheck(ctx); err != nil {
		s.healthy.Store(false)
		s.sendResponse(w, http.StatusServiceUnavailable, APIResponse{
			Success: false,
			Message: "Service unhealthy: " + err.Error(),
		})
		return
	}

	s.healthy.Store(true)
	s.sendResponse(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Service healthy",
	})
}

// loggingMiddleware logs each request with timing information
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("Started %s %s", r.Method, r.URL.Path)
		
		// Create a custom response writer to capture the status code
		wrapped := wrapResponseWriter(w)
		next.ServeHTTP(wrapped, r)
		
		log.Printf("Completed %s %s [%d] in %v", r.Method, r.URL.Path, wrapped.status, time.Since(start))
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	status int
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, status: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// batchUploadHandler processes a batch of company data
func (s *Server) batchUploadHandler(w http.ResponseWriter, r *http.Request) {
	if !s.healthy.Load() {
		s.sendResponse(w, http.StatusServiceUnavailable, APIResponse{
			Success: false,
			Message: "Service is not healthy",
		})
		return
	}

	var req CompanyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendResponse(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "Invalid request body: " + err.Error(),
		})
		return
	}

	if len(req.Companies) == 0 {
		s.sendResponse(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "No companies provided",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	processedCount, err := s.batchProcessor.ProcessBatch(ctx, req.Companies)
	if err != nil {
		s.sendResponse(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: "Failed to process batch: " + err.Error(),
		})
		return
	}

	s.sendResponse(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Batch processed successfully",
		Data: map[string]interface{}{
			"processed_count": processedCount,
		},
	})
}

// updateTreatedHandler updates the treated status for a company
func (s *Server) updateTreatedHandler(w http.ResponseWriter, r *http.Request) {
	companyName := r.URL.Query().Get("name")
	if companyName == "" {
		s.sendResponse(w, http.StatusBadRequest, APIResponse{
			Success: false,
			Message: "Company name is required",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.batchProcessor.UpdateTreatedField(ctx, companyName); err != nil {
		s.sendResponse(w, http.StatusInternalServerError, APIResponse{
			Success: false,
			Message: "Failed to update treated field: " + err.Error(),
		})
		return
	}

	s.sendResponse(w, http.StatusOK, APIResponse{
		Success: true,
		Message: "Company treated field updated successfully",
	})
}

// sendResponse sends a JSON response
func (s *Server) sendResponse(w http.ResponseWriter, status int, response APIResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
	}
}

func main() {
	// Initialize MongoDB connection
	bp, err := middleware.NewBatchProcessor(
		"mongodb://localhost:27017",
		"companies_db",
		"companies",
		100, // batch size
		4,   // number of workers
	)
	if err != nil {
		log.Fatal("Failed to initialize batch processor:", err)
	}

	// Create and configure the server
	server := NewServer(bp)
	httpServer := &http.Server{
		Addr:         ":8080",
		Handler:      server.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown handling
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}
		if err := bp.Close(ctx); err != nil {
			log.Printf("MongoDB connection closure error: %v", err)
		}
	}()

	// Start the server
	log.Printf("Server starting on port 8080")
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal("Server error:", err)
	}
}