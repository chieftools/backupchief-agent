package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

const invalidEndpointMessage = "S3 requires a public HTTPS endpoint on port 443 " +
	"without a path, credentials, query or fragment"

type Target struct {
	Host string
	Port uint16
}

var blockedNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/3"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

var publicIPv6Network = netip.MustParsePrefix("2000::/3")

func Public(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || (ip.Is6() && !publicIPv6Network.Contains(ip)) {
		return false
	}

	for _, prefix := range blockedNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}

	return true
}

func Endpoint(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New(invalidEndpointMessage)
	}

	if endpoint.Scheme != "https" ||
		endpoint.Hostname() == "" ||
		endpoint.User != nil ||
		endpoint.RawQuery != "" ||
		endpoint.ForceQuery ||
		endpoint.Fragment != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") ||
		endpoint.RawPath != "" ||
		(endpoint.Port() != "" && endpoint.Port() != "443") {
		return nil, errors.New(invalidEndpointMessage)
	}

	authority, err := Authority(Target{Host: endpoint.Hostname(), Port: 443})
	if err != nil {
		return nil, errors.New("S3 endpoint is not public")
	}

	endpoint.Host = authority
	endpoint.Path = ""

	return endpoint, nil
}

func Authority(target Target) (string, error) {
	host := strings.ToLower(strings.TrimSpace(target.Host))
	if target.Port == 0 || host == "" || strings.HasSuffix(host, ".") || host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return "", errors.New("storage target is not public")
	}

	for _, character := range host {
		if !((character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			character == '.' ||
			character == '-' ||
			character == ':') {
			return "", errors.New("invalid storage hostname")
		}
	}

	if ip, err := netip.ParseAddr(host); err == nil && !Public(ip) {
		return "", errors.New("storage target is not public")
	} else if err != nil {
		if strings.Contains(host, ":") {
			return "", errors.New("invalid storage hostname")
		}
		for _, label := range strings.Split(host, ".") {
			if label == "" || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
				return "", errors.New("invalid storage hostname")
			}
		}
	}

	return net.JoinHostPort(host, fmt.Sprintf("%d", target.Port)), nil
}

type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

func Addresses(ctx context.Context, resolver Resolver, host string) ([]netip.Addr, error) {
	ips, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("storage DNS lookup failed")
	}

	for _, ip := range ips {
		if !Public(ip) {
			return nil, errors.New("storage DNS returned a prohibited address")
		}
	}

	return ips, nil
}
