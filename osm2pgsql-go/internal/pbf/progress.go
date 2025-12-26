package pbf

import (
	"context"
	"time"
)

// ProgressTicker calls a function periodically for progress updates
type ProgressTicker struct {
	ctx      context.Context
	callback func()
	interval time.Duration
}

// NewProgressTicker creates a new progress ticker
func NewProgressTicker(ctx context.Context, callback func()) *ProgressTicker {
	return &ProgressTicker{
		ctx:      ctx,
		callback: callback,
		interval: 500 * time.Millisecond,
	}
}

// Run starts the ticker
func (p *ProgressTicker) Run() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.callback()
		}
	}
}
