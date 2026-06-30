package deveco

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
)

// CallbackData holds the OAuth callback parameters from Huawei DevEco.
type CallbackData struct {
	TempToken string
	SiteID    string
	Quit      string
}

// CallbackServer handles the local HTTP server for DevEco OAuth callback.
type CallbackServer struct {
	server             *http.Server
	port               int
	clientSecret       string
	baseURL            string
	successRedirectURL string
	failedRedirectURL  string
	resultCh           chan *CallbackData
	errCh              chan error
}

// NewCallbackServer creates a new OAuth callback server.
func NewCallbackServer(port int, clientSecret, baseURL, successRedirect, failedRedirect string) *CallbackServer {
	return &CallbackServer{
		port:               port,
		clientSecret:       clientSecret,
		baseURL:            baseURL,
		successRedirectURL: successRedirect,
		failedRedirectURL:  failedRedirect,
		resultCh:           make(chan *CallbackData, 1),
		errCh:              make(chan error, 1),
	}
}

// Start begins listening for the OAuth callback on localhost.
func (s *CallbackServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", s.handleCallback)

	s.server = &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", s.port),
		Handler: mux,
	}

	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("deveco callback server: listen: %w", err)
	}

	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Errorf("deveco callback server: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the callback server.
func (s *CallbackServer) Stop() error {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}

// WaitForCallback blocks until the OAuth callback is received or timeout expires.
func (s *CallbackServer) WaitForCallback(ctx context.Context, timeout time.Duration) (*CallbackData, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case data := <-s.resultCh:
		return data, nil
	case err := <-s.errCh:
		return nil, err
	case <-timer.C:
		return nil, fmt.Errorf("deveco callback: timeout after %v", timeout)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *CallbackServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	var params url.Values

	if r.Method == http.MethodPost {
		body, err := io.ReadAll(r.Body)
		if err == nil && len(body) > 0 {
			if vals, parseErr := url.ParseQuery(string(body)); parseErr == nil {
				params = vals
			}
		}
		r.Body.Close()
	}

	if params == nil {
		params = r.URL.Query()
	}

	code := params.Get("code")
	tempToken := params.Get("tempToken")
	siteID := params.Get("siteId")
	quit := params.Get("quit")

	if code != s.clientSecret {
		log.Warn("deveco callback: code mismatch, ignoring")
		http.Error(w, "Invalid code", http.StatusBadRequest)
		return
	}

	if quit == "true" || quit == "access_denied" {
		log.Info("deveco callback: user cancelled login")
		select {
		case s.errCh <- fmt.Errorf("login cancelled by user"):
		default:
		}
		http.Redirect(w, r, s.baseURL+"/"+s.failedRedirectURL, http.StatusFound)
		return
	}

	if tempToken == "" || siteID == "" {
		log.Error("deveco callback: missing tempToken or siteId")
		select {
		case s.errCh <- fmt.Errorf("login failed: missing credentials"):
		default:
		}
		http.Redirect(w, r, s.baseURL+"/"+s.failedRedirectURL, http.StatusFound)
		return
	}

	if siteID != "1" {
		log.Errorf("deveco callback: unsupported region siteId=%s", siteID)
		select {
		case s.errCh <- fmt.Errorf("unsupported region: only China site accounts are supported"):
		default:
		}
		http.Redirect(w, r, s.baseURL+"/"+s.failedRedirectURL, http.StatusFound)
		return
	}

	select {
	case s.resultCh <- &CallbackData{TempToken: tempToken, SiteID: siteID, Quit: quit}:
	default:
	}
	http.Redirect(w, r, s.baseURL+"/"+s.successRedirectURL, http.StatusFound)
}
