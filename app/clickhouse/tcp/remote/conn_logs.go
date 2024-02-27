package remote

import (
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"time"
)

type Log struct {
	Time      time.Time
	TimeMicro uint32
	Hostname  string
	QueryID   string
	ThreadID  uint64
	Priority  int8
	Source    string
	Text      string
}

func (c *connect) logs() ([]Log, error) {
	block, err := c.readData(proto.ServerLog, false)
	if err != nil {
		return nil, err
	}
	c.debugf("[logs] rows=%d", block.Rows())
	var (
		logs  []Log
		names = block.ColumnsNames()
	)
	for r := 0; r < block.Rows(); r++ {
		var log Log
		for i, b := range block.Columns {
			switch names[i] {
			case "event_time":
				if err := b.ScanRow(&log.Time, r); err != nil {
					return nil, err
				}
			case "event_time_microseconds":
				if err := b.ScanRow(&log.TimeMicro, r); err != nil {
					return nil, err
				}
			case "host_name":
				if err := b.ScanRow(&log.Hostname, r); err != nil {
					return nil, err
				}
			case "query_id":
				if err := b.ScanRow(&log.QueryID, r); err != nil {
					return nil, err
				}
			case "thread_id":
				if err := b.ScanRow(&log.ThreadID, r); err != nil {
					return nil, err
				}
			case "priority":
				if err := b.ScanRow(&log.Priority, r); err != nil {
					return nil, err
				}
			case "source":
				if err := b.ScanRow(&log.Source, r); err != nil {
					return nil, err
				}
			case "text":
				if err := b.ScanRow(&log.Text, r); err != nil {
					return nil, err
				}
			}
		}
		logs = append(logs, log)
	}
	return logs, nil
}
