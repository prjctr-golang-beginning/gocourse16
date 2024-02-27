package remote

import (
	"context"
	"gocourse16/app/clickhouse/tcp/lib/proto"
	"time"
)

func (c *connect) query(ctx context.Context, release func(*connect, error), query string, args ...interface{}) (*rows, error) {
	var (
		options   = queryOptions(ctx)
		onProcess = options.onProcess()
		body, err = bind(c.server.Timezone, query, args...)
	)

	if err != nil {
		release(c, err)
		return nil, err
	}

	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}

	if err = c.sendQuery(body, &options); err != nil {
		release(c, err)
		return nil, err
	}

	init, err := c.firstBlock(ctx, onProcess)

	if err != nil {
		release(c, err)
		return nil, err
	}

	var (
		errors = make(chan error)
		stream = make(chan *proto.Block, 2)
	)

	go func() {
		onProcess.data = func(b *proto.Block) {
			stream <- b
		}
		err := c.process(ctx, onProcess, nil)
		if err != nil {
			errors <- err
		}
		close(stream)
		close(errors)
		release(c, err)
	}()

	return &rows{
		block:     init,
		stream:    stream,
		errors:    errors,
		columns:   init.ColumnsNames(),
		structMap: c.structMap,
	}, nil
}

func (c *connect) queryRow(ctx context.Context, release func(*connect, error), query string, args ...interface{}) *row {
	rows, err := c.query(ctx, release, query, args...)
	if err != nil {
		return &row{
			err: err,
		}
	}
	return &row{
		rows: rows,
	}
}
