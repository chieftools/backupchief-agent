package egress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type Proxy struct {
	URL         string
	server      *http.Server
	listener    net.Listener
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	closed      bool
	hosts       map[string]struct{}
	resolver    Resolver
	dial        func(context.Context, string, string) (net.Conn, error)
	slots       chan struct{}
}

func Start(ctx context.Context, endpoint string) (*Proxy, error) {
	return StartMany(ctx, []string{endpoint})
}

func StartMany(ctx context.Context, endpoints []string) (*Proxy, error) {
	authorities := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		authorizedEndpoint, err := Endpoint(endpoint)
		if err != nil {
			return nil, err
		}
		authorities = append(authorities, authorizedEndpoint.Host)
	}
	return startAuthorities(ctx, authorities)
}

func StartTargets(ctx context.Context, targets []Target) (*Proxy, error) {
	authorities := make([]string, 0, len(targets))
	for _, target := range targets {
		authority, err := Authority(target)
		if err != nil {
			return nil, err
		}
		authorities = append(authorities, authority)
	}
	return startAuthorities(ctx, authorities)
}

func startAuthorities(ctx context.Context, authorities []string) (*Proxy, error) {
	hosts := make(map[string]struct{}, len(authorities))
	for _, authority := range authorities {
		hosts[authority] = struct{}{}
	}
	if len(hosts) == 0 {
		return nil, errors.New("at least one egress authority is required")
	}

	proxy := &Proxy{
		hosts:       hosts,
		resolver:    net.DefaultResolver,
		dial:        (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		connections: make(map[net.Conn]struct{}),
		slots:       make(chan struct{}, 16),
	}

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	proxy.listener = listener

	proxy.URL = "http://" + proxy.listener.Addr().String()
	proxy.server = &http.Server{
		Handler:           http.HandlerFunc(proxy.serve),
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    4096,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}

	go func() {
		_ = proxy.server.Serve(proxy.listener)
	}()

	return proxy, nil
}

func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	for conn := range p.connections {
		_ = conn.Close()
	}
	p.mu.Unlock()

	_ = p.server.Close()
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect || r.Host != r.URL.Host {
		http.Error(w, "destination denied", http.StatusForbidden)
		return
	}
	if _, allowed := p.hosts[r.Host]; !allowed {
		http.Error(w, "destination denied", http.StatusForbidden)
		return
	}

	select {
	case p.slots <- struct{}{}:
		defer func() {
			<-p.slots
		}()
	default:
		http.Error(w, "connection limit", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	host, port, err := net.SplitHostPort(r.Host)
	if err != nil {
		http.Error(w, "destination denied", http.StatusForbidden)
		return
	}
	ips, err := Addresses(ctx, p.resolver, host)
	if err != nil {
		http.Error(w, "destination denied", http.StatusForbidden)
		return
	}

	var upstream net.Conn

	for _, ip := range ips {
		upstream, err = p.dial(ctx, "tcp", net.JoinHostPort(ip.Unmap().String(), port))
		if err == nil {
			break
		}
	}

	if upstream == nil {
		http.Error(w, "connection failed", http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "tunnel unavailable", http.StatusInternalServerError)
		return
	}

	client, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer client.Close()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.connections[client] = struct{}{}
	p.connections[upstream] = struct{}{}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.connections, client)
		delete(p.connections, upstream)
		p.mu.Unlock()
	}()

	if _, err = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err = buffered.Flush(); err != nil {
		return
	}

	done := make(chan error, 2)

	go func() {
		_, err := io.Copy(upstream, buffered)
		done <- err
	}()

	go func() {
		_, err := io.Copy(client, upstream)
		done <- err
	}()

	select {
	case <-done:
	case <-r.Context().Done():
	}

	_ = client.Close()
	_ = upstream.Close()
}
