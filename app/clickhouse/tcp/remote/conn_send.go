package remote

import (
	"context"
	"io"
	"time"
)

func (c *connect) send(ctx context.Context, packet []byte, consumer io.Writer) error {
	var (
		options = queryOptions(ctx)
		//body, err = bind(c.server.Timezone, query, args...)
	)
	//if err != nil {
	//	return err
	//}
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if err := c.sendPacket(packet /*, &options*/); err != nil {
		return err
	}
	return c.process(ctx, options.onProcess(), consumer)
}
