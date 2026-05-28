package main

import (
	"crypto/tls"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct{
		URL    *url.URL
		IsAlive bool;
		mux     sync.RWMutex;
}

type ServerPool struct {
	backends []*Backend      // The actual list of servers (The Phonebook)
	mu       sync.RWMutex    // The lock that protects the backends list
	
	// atomic.Uint64 safely tracks request numbers across thousands 
	// of simultaneous web requests without needing a Mutex lock!
	requestCounter atomic.Uint64 
}


func main() {
    // 1. Define your multiple backend ports
    rawBackends := []string{
        "https://localhost:8080",
        "https://localhost:8081",
        "https://localhost:8082",
    }
    // Parse them into actual url.URL objects
    var initialBackends []*Backend
    for _, raw := range rawBackends {
        parsed, err := url.Parse(raw)
        if err != nil {
            log.Fatalf("Invalid backend URL %s: %v", raw, err)
        }
        initialBackends = append(initialBackends, &Backend{
			URL:parsed,
			IsAlive: true,
		})
    }

    // 2. Create a thread-safe counter for Round-Robin routing
    // atomic.Uint64 is safe to use across thousands of concurrent requests 
	pool:=&ServerPool{
		backends: initialBackends,
	}

	go pool.healthCheck()

    // 3. Initialize the ReverseProxy
    proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetXForwarded()
			
			var targetURL *url.URL
		
			backend:=pool.GetNextPeer();

			targetURL=backend.URL;

			if targetURL == nil {
				log.Println("CRITICAL: All backends are dead!")
				// Give the proxy a safe dummy URL so it doesn't crash with the scheme error
				dummyURL, _ := url.Parse("http://0.0.0.0")
				pr.SetURL(dummyURL)
				return 
			}
			
			pr.SetURL(targetURL)
			log.Printf("Proxying %s request to backend: %s", pr.In.Method, targetURL.Host)
    	},

		Transport: &http.Transport{
            TLSClientConfig: &tls.Config{
                // Because we are using mkcert, the Load Balancer will 
                // trust the backend's internal certificate automatically!
                MinVersion: tls.VersionTLS12,
            },
        },
    
    // Catch the dummy URL (or any other connection failure) and send a friendly JSON error
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// ADD THIS LINE:
			log.Printf("Proxy error: %v", err) 
			
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error": "Service Unavailable. All backends are down."}`))
		},
	}

    // 4. Start the load balancer on port 9000
    log.Println("Load Balancer is running on http://localhost:9000",)
    if err := http.ListenAndServeTLS(
    ":9000", 
    "api.mycoolstartup.com.pem", 
    "api.mycoolstartup.com-key.pem", 
    proxy,
); err != nil {
    log.Fatalf("Proxy server failed to start: %v", err)
}
}

func (b *Backend) SetAlive(alive bool) {
    b.mux.Lock()
    b.IsAlive = alive
    b.mux.Unlock()
}

// GetAlive safely reads the state
func (b *Backend) GetAlive() bool {
    b.mux.RLock()
    alive := b.IsAlive
    b.mux.RUnlock()
    return alive
}


func (p *ServerPool) healthCheck() {
    t := time.NewTicker(3 * time.Second)
    
    // Create an HTTP client with a strict timeout
    client := http.Client{
        Timeout: 2 * time.Second,
    }

    for range t.C {


		p.mu.RLock()
		snapshot:= make([]*Backend, len(p.backends));
		copy(snapshot,p.backends)
		p.mu.RUnlock();

		var wg sync.WaitGroup


        for _, b := range snapshot {
            // Ping the backend. You can use b.URL.String() + "/health" if you have a specific route.
			wg.Add(1)

			go func(backend *Backend){
				 resp, err := client.Get(b.URL.String() + "/health" )
				defer wg.Done()
				if err != nil {
						log.Printf("HealthCheck failed for %s: %v", b.URL.String(), err)
						b.SetAlive(false)
					} else if resp.StatusCode >= 400 {
						// ADD THIS LINE:
						log.Printf("HealthCheck failed for %s: Got HTTP %d", b.URL.String(), resp.StatusCode)
						b.SetAlive(false)
					} else {
						b.SetAlive(true)
					}
					
					if resp != nil {
						resp.Body.Close() // Always close the body to prevent memory leaks
					} 
			}(b)
        }

		wg.Wait()
    }
}

func (p *ServerPool) GetNextPeer() *Backend {
    // 1. Take a safe snapshot (protects against the Autoscaler!)
    p.mu.RLock()
    snapshot := make([]*Backend, len(p.backends))
    copy(snapshot, p.backends)
    p.mu.RUnlock()

    if len(snapshot) == 0 {
        return nil
    }

    // 2. Loop through the snapshot to find an alive server
    for i := 0; i < len(snapshot); i++ {
        // We moved the requestCounter inside the ServerPool!
        currentCount := p.requestCounter.Add(1)
        targetIndex := currentCount % uint64(len(snapshot))
        backend := snapshot[targetIndex]
        
        if backend.GetAlive() {
            return backend
        }
    }

    return nil // All servers are dead
}

func parseURL(urlStr string) *url.URL {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		log.Fatalf("Failed to parse URL %s: %v", urlStr, err)
	}
	return parsed
}