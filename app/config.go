package app

import (
	"go.uber.org/atomic"
	"gocourse16/app/driver"
)

const (
	BlackList SpecialTableType = `black`
	WhiteList SpecialTableType = `white`
)

type (
	SpecialTableType string

	Config struct {
		Env     string
		Debug   bool `yaml:"debug"`
		Http    Http
		Listen  Networks
		Targets Targets
	}

	Http struct {
		Port string
	}

	Addr struct {
		Host string
		Port string
	}

	NetworkOpts struct {
		Addr    Addr
		Allowed []string

		// default doesn't work now
		ConcurrencyLimit int     `yaml:"concurrency_limit" default:"10"`
		MaxWaitTime      float64 `yaml:"max_wait_time" default:"10"`  // not needed if ConcurrencyLimit is 0
		MaxQueueSize     int     `yaml:"max_queue_size" default:"10"` // not needed if ConcurrencyLimit is 0
	}

	Networks struct {
		Tcp  []NetworkOpts
		Http []NetworkOpts
	}

	Targets struct {
		TableGroups [][]string `yaml:"table_groups"`
		Clusters    []RemoteTarget
	}

	RemoteTarget struct {
		Host          string
		Port          string
		User          string
		Pass          string
		SpecialTables SpecialTables `yaml:"special_tables"`
	}

	SpecialTables struct {
		Type SpecialTableType
		List [][]string
	}

	RouteRule func([]*RemoteMaster, driver.SqlPartsExtractor) *RemoteMaster
)

var rrCounter = atomic.NewInt32(0)

func (st *SpecialTables) Tables() []string {
	var list []string

	for i := range st.List {
		list = append(list, st.List[i]...)
	}

	return list
}

// WeightRouteRule looking for cluster, where is the most heavy table from SQL query
func WeightRouteRule(cps []*RemoteMaster, se driver.SqlPartsExtractor) (max *RemoteMaster) {
	if len(cps) == 1 {
		return cps[0]
	}

	var maxWeight uint64
	ut := se.UsedTables()
	for i := range cps {
		for _, rt := range cps[i].tables {
			for _, t := range ut {
				if rt.Name == t {
					if rt.B > maxWeight {
						maxWeight = rt.B
						max = cps[i]
						// TODO: if difference bw tables less then 5% - they are eq -> RR
					}
				}
			}
		}
	}

	return max
}

func RoundRobinRouteRule(cps []*RemoteMaster, _ driver.SqlPartsExtractor) *RemoteMaster {
	if rrCounter.Load() >= int32(len(cps)) {
		rrCounter.Store(0)
	}
	defer rrCounter.Inc()

	return cps[rrCounter.Load()]
}
