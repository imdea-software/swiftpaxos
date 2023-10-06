package config

import (
	"bufio"
	"strings"
)

type ProxyInfo struct {
	// server alias -> client addrs
	locals map[string]map[string]struct{}
	// server alias -> client addrs
	proxies map[string]map[string]struct{}
	// client addr -> server alias
	servers map[string]string
}

const REMOTE = "none"

func ReadProxyInfo(c *Config, s *bufio.Scanner, end string) *ProxyInfo {
	p := &ProxyInfo{
		locals:  make(map[string]map[string]struct{}),
		proxies: make(map[string]map[string]struct{}),
		servers: make(map[string]string),
	}

	alias := ""
	for s.Scan() {
		fs := strings.Fields(s.Text())
		if l := len(fs); l < 1 {
			continue
		} else if l > 1 {
			if fs[0] == "server_alias" {
				alias = fs[1]
				p.locals[alias] = make(map[string]struct{})
				p.proxies[alias] = make(map[string]struct{})
			} else if strings.Contains(fs[1], "local") {
				addr := c.ClientAddrs[fs[0]]
				p.locals[alias][addr] = struct{}{}
				p.proxies[alias][addr] = struct{}{}
				p.servers[addr] = alias
			}
		} else if l == 1 {
			if fs[0] == end {
				return p
			}
			p.proxies[alias][c.ClientAddrs[fs[0]]] = struct{}{}
		}
	}

	return p
}

func (p *ProxyInfo) IsProxy(serverAlias, clientAddr string) bool {
	_, exists := p.proxies[serverAlias]
	if !exists {
		return false
	}
	_, exists = p.proxies[serverAlias][clientAddr]
	return exists
}

func (p *ProxyInfo) IsLocal(serverAlias, clientAddr string) bool {
	_, exists := p.locals[serverAlias]
	if !exists {
		return false
	}
	_, exists = p.locals[serverAlias][clientAddr]
	return exists
}

func (p *ProxyInfo) ProxyOf(clientAddr string) string {
	for server, ps := range p.proxies {
		if _, exists := ps[clientAddr]; exists {
			return server
		}
	}
	return "none"
}

func (p *ProxyInfo) LocalWith(clientAddr string) string {
	alias, exists := p.servers[clientAddr]
	if !exists {
		return REMOTE
	}
	return alias
}

func (p *ProxyInfo) Servers() []string {
	if p == nil {
		return nil
	}
	s := []string{}
	for server := range p.proxies {
		s = append(s, server)
	}
	return s
}

func (p *ProxyInfo) ServerClients(server string) []string {
	if p == nil || p.proxies == nil {
		return nil
	}
	c := []string{}
	for client := range p.proxies[server] {
		c = append(c, client)
	}
	return c
}

func (p *ProxyInfo) Clients() []string {
	if p == nil {
		return nil
	}
	c := []string{}
	for client := range p.servers {
		c = append(c, client)
	}
	return c
}
