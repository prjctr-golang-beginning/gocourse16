package remote

import (
	"context"
	"time"
)

func (c *connect) exec(ctx context.Context, query string, args ...interface{}) error {
	var (
		options   = queryOptions(ctx)
		body, err = bind(c.server.Timezone, query, args...)
	)
	if err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if err := c.sendQuery(body, &options); err != nil {
		return err
	}
	return c.process(ctx, options.onProcess(), nil)
}
