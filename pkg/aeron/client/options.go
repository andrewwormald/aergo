package client

import "time"

// Option configures the Aeron client.
type Option func(*config)

type config struct {
	Dir            string
	DriverTimeout  time.Duration
	IdleStrategy   func(workCount int)
}

func defaultConfig() config {
	return config{
		DriverTimeout: 10 * time.Second,
	}
}

// WithDir sets the Aeron media driver directory.
func WithDir(dir string) Option {
	return func(c *config) {
		c.Dir = dir
	}
}

// WithDriverTimeout sets the timeout for connecting to the media driver.
func WithDriverTimeout(d time.Duration) Option {
	return func(c *config) {
		c.DriverTimeout = d
	}
}

// WithIdleStrategy sets a custom idle strategy for the conductor loop.
func WithIdleStrategy(fn func(workCount int)) Option {
	return func(c *config) {
		c.IdleStrategy = fn
	}
}
