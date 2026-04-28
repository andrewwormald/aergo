package client

import (
	"fmt"
	"unsafe"

	"github.com/andrewwormald/aergo/pkg/aeron/driver"
)

// Client is the main Aeron client connecting to a media driver.
type Client struct {
	ctx    unsafe.Pointer
	client unsafe.Pointer
	cfg    config
	closed bool
}

// New creates and starts an Aeron client.
func New(opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	var ctx unsafe.Pointer
	if err := driver.ContextInit(&ctx); err != nil {
		return nil, fmt.Errorf("context init: %w", err)
	}

	if cfg.Dir != "" {
		if err := driver.ContextSetDir(ctx, cfg.Dir); err != nil {
			driver.ContextClose(ctx)
			return nil, fmt.Errorf("context set dir: %w", err)
		}
	}

	if cfg.DriverTimeout > 0 {
		ms := uint64(cfg.DriverTimeout.Milliseconds())
		if err := driver.ContextSetDriverTimeoutMs(ctx, ms); err != nil {
			driver.ContextClose(ctx)
			return nil, fmt.Errorf("context set driver timeout: %w", err)
		}
	}

	// Set error handler
	if err := driver.ContextSetErrorHandler(ctx, errorHandlerCCallback, nil); err != nil {
		driver.ContextClose(ctx)
		return nil, fmt.Errorf("context set error handler: %w", err)
	}

	var client unsafe.Pointer
	if err := driver.ClientInit(&client, ctx); err != nil {
		driver.ContextClose(ctx)
		return nil, fmt.Errorf("client init: %w", err)
	}

	if err := driver.ClientStart(client); err != nil {
		driver.ClientClose(client)
		driver.ContextClose(ctx)
		return nil, fmt.Errorf("client start: %w", err)
	}

	return &Client{
		ctx:    ctx,
		client: client,
		cfg:    cfg,
	}, nil
}

// AddPublication creates a publication on the given channel and stream.
// Blocks until the publication is established.
func (c *Client) AddPublication(uri string, streamId int32) (*Publication, error) {
	var async unsafe.Pointer
	if err := driver.AsyncAddPublication(&async, c.client, uri, streamId); err != nil {
		return nil, fmt.Errorf("async add publication: %w", err)
	}

	// Poll conductor while waiting for publication
	pub, err := awaitPublication(async)
	if err != nil {
		return nil, err
	}
	pub.uri = uri
	pub.streamId = streamId
	return pub, nil
}

// AddExclusivePublication creates an exclusive publication on the given channel and stream.
// Exclusive publications provide better throughput for single-writer scenarios.
func (c *Client) AddExclusivePublication(uri string, streamId int32) (*ExclusivePublication, error) {
	var async unsafe.Pointer
	if err := driver.AsyncAddExclusivePublication(&async, c.client, uri, streamId); err != nil {
		return nil, fmt.Errorf("async add exclusive publication: %w", err)
	}

	pub, err := awaitExclusivePublication(async)
	if err != nil {
		return nil, err
	}
	pub.uri = uri
	pub.streamId = streamId
	return pub, nil
}

// AddSubscription creates a subscription on the given channel and stream.
// Blocks until the subscription is established.
func (c *Client) AddSubscription(uri string, streamId int32) (*Subscription, error) {
	var async unsafe.Pointer
	if err := driver.AsyncAddSubscription(
		&async, c.client, uri, streamId,
		imageAvailableCCallback, nil,
		imageUnavailableCCallback, nil,
	); err != nil {
		return nil, fmt.Errorf("async add subscription: %w", err)
	}

	sub, err := awaitSubscription(async)
	if err != nil {
		return nil, err
	}
	sub.uri = uri
	sub.streamId = streamId
	return sub, nil
}

// DoWork performs conductor work. Returns the number of work items processed.
func (c *Client) DoWork() int {
	return int(driver.ClientDoWork(c.client))
}

// NextCorrelationId generates the next correlation ID.
func (c *Client) NextCorrelationId() int64 {
	return driver.NextCorrelationId(c.client)
}

// ClientId returns this client's unique ID.
func (c *Client) ClientId() int64 {
	return driver.ClientId(c.client)
}

// Close shuts down the client and releases all resources.
func (c *Client) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true

	if err := driver.ClientClose(c.client); err != nil {
		return fmt.Errorf("client close: %w", err)
	}
	if err := driver.ContextClose(c.ctx); err != nil {
		return fmt.Errorf("context close: %w", err)
	}
	return nil
}
