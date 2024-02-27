package remote

func (c *connect) sendPacket(body []byte) error {
	c.debugf("[send packet] compression=%t %s", c.compression, body)
	if err := c.encoder.Raw(body); err != nil {
		return err
	}
	if c.compression {
		c.stream.Compress(true)
		defer func() {
			c.stream.Compress(false)
			c.encoder.Flush()
		}()
	}
	return c.encoder.Flush()
}
