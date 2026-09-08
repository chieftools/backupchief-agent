package restic

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestPinnedResticRejectsRedirectToLoopback(t *testing.T) {
	runner := testRunner(t)
	binary, err := Binary(context.Background(), runner.State, runner.DevelopmentBinary)
	if err != nil {
		t.Fatal(err)
	}

	var forbiddenCalls, endpointCalls atomic.Int32

	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forbiddenCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer forbidden.Close()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	certificate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "objects.example.test"},
		DNSNames:              []string{"objects.example.test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		endpointCalls.Add(1)
		http.Redirect(w, request, forbidden.URL+"/metadata", http.StatusTemporaryRedirect)
	}))
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{der},
				PrivateKey:  key,
			},
		},
		MinVersion: tls.VersionTLS12,
	}
	server.StartTLS()
	defer server.Close()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != "CONNECT" || request.Host != "objects.example.test:443" {
			http.Error(w, "unexpected destination", http.StatusForbidden)
			return
		}

		upstream, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
		if err != nil {
			http.Error(w, "unavailable", http.StatusBadGateway)
			return
		}
		defer upstream.Close()

		client, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer client.Close()

		_, _ = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = buffer.Flush()

		done := make(chan struct{}, 1)

		go func() {
			_, _ = io.Copy(upstream, buffer)
			done <- struct{}{}
		}()

		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		_ = upstream.Close()
		<-done
	}))
	defer proxy.Close()

	root := t.TempDir()
	ca := filepath.Join(root, "ca.pem")
	password := filepath.Join(root, "password")

	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(password, []byte("synthetic-password"), 0600); err != nil {
		t.Fatal(err)
	}

	request := Request{
		Version:   1,
		Operation: "snapshots",
		Connection: Connection{
			Driver:    "s3",
			Endpoint:  "https://objects.example.test",
			Bucket:    "synthetic-bucket",
			Prefix:    "repository",
			Region:    "auto",
			AccessKey: "synthetic-access",
			SecretKey: "synthetic-secret",
		},
		Password:       "synthetic-password",
		TimeoutSeconds: 5,
	}

	args, env, err := request.arguments(password, "", root, false)
	if err != nil {
		t.Fatal(err)
	}

	args = append([]string{"--cacert", ca}, args...)

	command := exec.Command(binary, args...)
	command.Env = append(env, "HTTPS_PROXY="+proxy.URL, "HTTP_PROXY="+proxy.URL, "NO_PROXY=")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := runProcess(ctx, command, request)

	if result.Outcome == "complete" || endpointCalls.Load() == 0 || forbiddenCalls.Load() != 0 {
		t.Fatalf(
			"redirect enforcement: endpoint=%d forbidden=%d result=%+v",
			endpointCalls.Load(),
			forbiddenCalls.Load(),
			result,
		)
	}
}
