package egress

import (
	"context"
	"net/netip"
	"testing"
)

type fakeResolver struct {
	addresses []netip.Addr
}

func (r fakeResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return r.addresses, nil
}

func TestPublicAddresses(t *testing.T) {
	prohibitedAddresses := []string{
		"127.0.0.1",
		"10.2.3.4",
		"172.16.0.1",
		"192.168.5.1",
		"169.254.169.254",
		"100.64.0.1",
		"0.0.0.0",
		"198.18.1.1",
		"203.0.113.1",
		"224.0.0.1",
		"::1",
		"::ffff:127.0.0.1",
		"fc00::1",
		"fe80::1",
		"64:ff9b::a00:1",
		"2001:db8::1",
		"2002:a00:1::",
		"3fff::1",
	}

	for _, address := range prohibitedAddresses {
		if Public(netip.MustParseAddr(address)) {
			t.Errorf("accepted prohibited address %s", address)
		}
	}

	publicAddresses := []string{
		"1.1.1.1",
		"8.8.8.8",
		"2606:4700:4700::1111",
		"::ffff:1.1.1.1",
	}

	for _, address := range publicAddresses {
		if !Public(netip.MustParseAddr(address)) {
			t.Errorf("rejected public address %s", address)
		}
	}
}

func TestEndpointRestrictions(t *testing.T) {
	prohibitedEndpoints := []string{
		"http://objects.test",
		"https://objects.test:9000",
		"https://objects.test/path",
		"https://user:secret@objects.test",
		"https://objects.test?x=1",
		"https://objects.test#x",
		"https://127.0.0.1",
		"https://[::ffff:127.0.0.1]",
		"https://localhost",
		"https://objects.localhost",
		"https://objects.test.",
	}

	for _, endpoint := range prohibitedEndpoints {
		if _, err := Endpoint(endpoint); err == nil {
			t.Errorf("accepted %s", endpoint)
		}
	}

	if _, err := Endpoint("https://objects.example.test:443/"); err != nil {
		t.Fatal(err)
	}
}

func TestTargetRestrictions(t *testing.T) {
	if authority, err := Authority(Target{Host: "storage.example.test", Port: 2222}); err != nil || authority != "storage.example.test:2222" {
		t.Fatalf("unexpected authority %q: %v", authority, err)
	}
	if authority, err := Authority(Target{Host: "STORAGE.EXAMPLE.TEST", Port: 2222}); err != nil || authority != "storage.example.test:2222" {
		t.Fatalf("unexpected canonical authority %q: %v", authority, err)
	}

	for _, target := range []Target{
		{Host: "localhost", Port: 22},
		{Host: "127.0.0.1", Port: 22},
		{Host: "storage.example.test.", Port: 22},
		{Host: " storage.example.test", Port: 22},
		{Host: "storage.example.test\n", Port: 22},
		{Host: "storage.example.test", Port: 0},
	} {
		if _, err := Authority(target); err == nil {
			t.Fatalf("accepted target %#v", target)
		}
	}
}

func TestRejectMixedAndChangedDNS(t *testing.T) {
	public := netip.MustParseAddr("1.1.1.1")
	private := netip.MustParseAddr("127.0.0.1")

	resolver := fakeResolver{addresses: []netip.Addr{public}}
	if _, err := Addresses(context.Background(), resolver, "objects.test"); err != nil {
		t.Fatal(err)
	}

	for _, ips := range [][]netip.Addr{{public, private}, {private}, {}} {
		resolver := fakeResolver{addresses: ips}

		if _, err := Addresses(context.Background(), resolver, "objects.test"); err == nil {
			t.Fatal("accepted unsafe DNS")
		}
	}
}
