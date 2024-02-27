package remote

import (
	"context"
)

func (c *connect) asyncInsert(ctx context.Context, query string, wait bool) error {
	options := queryOptions(ctx)
	{
		options.settings["async_insert"] = 1
		options.settings["wait_for_async_insert"] = 0
		if wait {
			options.settings["wait_for_async_insert"] = 1
		}
	}
	if err := c.sendQuery(query, &options); err != nil {
		return err
	}
	return c.process(ctx, options.onProcess(), nil)
}
