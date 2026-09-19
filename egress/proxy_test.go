package egress

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxyPinsDialAndRejectsRebinding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy, err := StartMany(ctx, []string{"https://objects.example.test", "https://replica.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	var private atomic.Bool

	proxy.resolver = resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		if private.Load() {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("1.1.1.1")}, nil
	})

	var dials atomic.Int32

	proxy.dial = func(_ context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || address != "1.1.1.1:443" {
			t.Errorf("unvalidated dial %s %s", network, address)
		}
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			buffer := make([]byte, 4)
			if _, err := io.ReadFull(server, buffer); err == nil {
				_, _ = server.Write(buffer)
			}
		}()
		return client, nil
	}

	proxyURL, _ := url.Parse(proxy.URL)

	connect := func(host string) (net.Conn, *bufio.Reader) {
		client, err := net.DialTimeout("tcp", proxyURL.Host, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = client.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = fmt.Fprintf(client, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", host, host)
		return client, bufio.NewReader(client)
	}

	client, reader := connect("objects.example.test:443")
	line, _ := reader.ReadString('\n')
	if !strings.Contains(line, "200") {
		t.Fatal(line)
	}

	_, _ = reader.ReadString('\n')
	_, _ = client.Write([]byte("ping"))
	reply := make([]byte, 4)
	if _, err := io.ReadFull(reader, reply); err != nil || string(reply) != "ping" {
		t.Fatalf("tunnel: %q %v", reply, err)
	}

	_ = client.Close()
	client, reader = connect("replica.example.test:443")
	line, _ = reader.ReadString('\n')
	if !strings.Contains(line, "200") {
		t.Fatal(line)
	}
	_, _ = reader.ReadString('\n')
	_ = client.Close()
	private.Store(true)

	prohibitedHosts := []string{
		"objects.example.test:443",
		"127.0.0.1:443",
		"other.example.test:443",
		"objects.example.test:80",
	}

	for _, host := range prohibitedHosts {
		client, reader = connect(host)
		line, _ = reader.ReadString('\n')
		_ = client.Close()

		if !strings.Contains(line, "403") {
			t.Fatalf("unsafe tunnel %s: %s", host, line)
		}
	}

	if dials.Load() != 2 {
		t.Fatalf("opened %d sockets; only the two approved endpoint dials were allowed", dials.Load())
	}
}

func TestProxyDialsApprovedCustomPort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy, err := StartTargets(ctx, []Target{{Host: "files.example.test", Port: 2222}})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	proxy.resolver = resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("1.1.1.1")}, nil
	})
	proxy.dial = func(_ context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || address != "1.1.1.1:2222" {
			t.Fatalf("unexpected dial %s %s", network, address)
		}
		client, server := net.Pipe()
		go server.Close()
		return client, nil
	}

	proxyURL, _ := url.Parse(proxy.URL)
	client, err := net.DialTimeout("tcp", proxyURL.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, _ = fmt.Fprint(client, "CONNECT files.example.test:2222 HTTP/1.1\r\nHost: files.example.test:2222\r\n\r\n")
	line, _ := bufio.NewReader(client).ReadString('\n')
	if !strings.Contains(line, "200") {
		t.Fatal(line)
	}
}

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}
