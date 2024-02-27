package app

import (
	"gocourse16/app/log"
	"net"
)

type ConnFilter struct { // TODO: cover with tests
	ipNet []net.IPNet
	ip    []net.IP
}

func NewConnFilter(a []string) ConnFilter {
	filter := ConnFilter{[]net.IPNet{}, []net.IP{}}

	for i := range a {
		if _, ipNet, err := net.ParseCIDR(a[i]); err != nil { // means a[i] is IP
			if ip := net.ParseIP(a[i]); ip == nil {
				log.Panicf(`Incorrect element in allowed list: %s`, a[i])
			} else {
				filter.ip = append(filter.ip, net.IP(a[i]))
			}
		} else {
			filter.ipNet = append(filter.ipNet, *ipNet)
		}
	}

	return filter
}

func (c ConnFilter) IsAllowed(addr string) bool {
	if len(c.ipNet) == 0 && len(c.ip) == 0 {
		return true
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}

	consIp := net.ParseIP(host)

	for i := range c.ipNet {
		if c.ipNet[i].Contains(consIp) {
			return true
		}
	}

	for i := range c.ip {
		if c.ip[i].Equal(consIp) {
			return true
		}
	}

	return false
}
