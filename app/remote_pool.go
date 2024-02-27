package app

import (
	"context"
	"errors"
	"fmt"
	"gocourse16/app/clickhouse/tcp/remote"
	"gocourse16/app/driver"
	"gocourse16/app/log"
	"math/rand"
	"sync"
	"time"
)

// RemotePool is pool of target clusters with route and restrictions logic
type RemotePool struct {
	ctx        context.Context
	targets    []RemoteTarget
	routeRules []RouteRule
	// something like middlewares
	extensions []driver.Extension
	// connections, which passed health-check
	pool []*RemoteMaster
}

const healthCheckPeriodDefault = 10 * time.Second // TODO: move in config
const interrogatePeriodDefault = 4 * time.Hour    // TODO: move in config

var interrogateQuery = "SELECT concat(database, '.', table) as table,\n" +
	"sum(bytes_on_disk) AS b\n" +
	"FROM system.parts\n" +
	"WHERE active/* AND database NOT IN (....) [ OR ((database != AND table !=) ... ]*/\n" +
	"GROUP BY database,\n" +
	"table\n" +
	"ORDER BY b ASC"

// NewRemotePool creates pool, first RouteRule if default and executed last
func NewRemotePool(ctx context.Context, rts Targets, rr ...RouteRule) *RemotePool {
	if len(rr) < 1 {
		panic("Must at least one remote rule")
	}

	targets := make([]RemoteTarget, 0, len(rts.Clusters))
	targets = append(targets, rts.Clusters...)

	rules := make([]RouteRule, 0, len(rr))
	rules = append(rules, rr...)

	return &RemotePool{ctx: ctx, targets: targets, routeRules: rules}
}

func (s *RemotePool) Init() (err error) {
	for _, t := range s.targets {
		rm := NewRemoteMaster("default", t.User, t.Pass, fmt.Sprintf("%s:%s", t.Host, t.Port))
		rm.ownConn, err = rm.Acquire("default", t.User, t.Pass)
		rm.specialTables = NewSpecialTables(t.SpecialTables.Type, t.SpecialTables.Tables())
		if err != nil {
			break
		}
		s.pool = append(s.pool, rm)
	}

	return
}

func (s *RemotePool) HealthChecking() {
	t := time.NewTicker(healthCheckPeriodDefault)
	for {
		<-t.C
		s.HealthCheck()
	}
}

// HealthCheck returns number of health instances
func (s *RemotePool) HealthCheck() int {
	wg := &sync.WaitGroup{}
	wg.Add(len(s.pool))

	var health int
	for i := range s.pool {
		go func(rm *RemoteMaster, wg *sync.WaitGroup) {
			defer wg.Done()

			if err := rm.Ping(context.Background()); err != nil {
				if rm.Alive() {
					rm.alive.Store(false)
					log.Warnf("Exclude connection from pool: %s: %s", rm.opts.Addr, err)
				}
			} else {
				health++
				if !rm.Alive() {
					rm.alive.Store(true)
					log.Warnf("Include connection back in pool: %s", rm.opts.Addr)
				}
			}
		}(s.pool[i], wg)
	}

	wg.Wait()

	return health
}

// Interrogating collects information about remote hosts periodically
func (s *RemotePool) Interrogating() {
	t := time.NewTicker(interrogatePeriodDefault)
	for {
		<-t.C
		s.Interrogate()
	}
}

// Interrogate collects information about remote hosts
func (s *RemotePool) Interrogate() {
	log.Debug("Interrogating started")
	for i := range s.pool {
		if err := s.interrogate(s.pool[i]); err != nil {
			log.Errorf("Interrogate problem for %v: %s", s.pool[i].opts.Addr, err)
		}
	}
	log.Debug("Interrogating completed")
}

// Interrogate collects information about remote hosts
func (s *RemotePool) interrogate(inst *RemoteMaster) error {
	if !inst.Alive() {
		return nil
	}

	conn, err := inst.Acquire("default", inst.opts.Auth.Username, inst.opts.Auth.Password)
	if err != nil {
		return err
	}
	rows, err := conn.Query(s.ctx, interrogateQuery)
	if err != nil {
		return err
	}
	table := RemoteTable{}
	for rows.Next() {
		if err = rows.ScanStruct(&table); err != nil {
			return err
		}
		inst.PushTable(table)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if inst.serverVersion, err = conn.ServerVersion(); err != nil {
		return err
	}
	log.Infof("Interrogated successfully for %s", inst.opts.Addr)

	return nil
}

func (s *RemotePool) Extend(e driver.Extension) {
	s.extensions = append(s.extensions, e)
}

func (s *RemotePool) Acquire(ex driver.SqlPartsExtractor, db, user, pass string) (driver.Conn, error) {
	for _, e := range s.extensions {
		if err := e.Use(ex); err != nil {
			return nil, err
		}
	}

	aliveOnly := make([]*RemoteMaster, 0, len(s.pool))
	for i := range s.pool {
		if s.pool[i].Alive() {
			aliveOnly = append(aliveOnly, s.pool[i])
		}
	}
	// stack walk
	for i, j := 0, len(s.routeRules)-1; i <= j; i++ {
		master := s.routeRules[j-i](s.pool, ex)
		if master != nil {
			return master.Acquire(db, user, pass)
		}
	}

	return nil, errors.New(fmt.Sprintf("Conn Pool not defined"))
}

// ServerVersion returns server version suitable for every clusters (the lowest one?)
func (s *RemotePool) ServerVersion() *remote.ServerVersion {
	if len(s.pool) == 0 {
		log.Error("Server version not defined due to s.pool is empty")
		return &remote.ServerVersion{}
	}
	if len(s.pool) == 1 {
		return s.pool[0].serverVersion
	}
	r := rand.New(rand.NewSource(time.Now().Unix()))

	return s.pool[r.Intn(len(s.pool)-1)].serverVersion
}

// AliveNum gives number of alive clusters in pool
func (s *RemotePool) AliveNum() int {
	num := 0

	for i := range s.pool {
		if s.pool[i].Alive() {
			num++
		}
	}

	return num
}
