package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/audio"
	"github.com/wyw14/solo-035/internal/encoder"
	"github.com/wyw14/solo-035/internal/model"
	"github.com/wyw14/solo-035/internal/playout"
)

var ErrFailbackAborted = errors.New("failback aborted")

type Coordinator struct {
	state    *State
	audio    *audio.Switch
	encoders *encoder.Coordinator
	playout  *playout.Integration
	clock    func() time.Time
}

func NewCoordinator(state *State, audioSwitch *audio.Switch, encoders *encoder.Coordinator, playoutIntegration *playout.Integration, clock func() time.Time) *Coordinator {
	return &Coordinator{state: state, audio: audioSwitch, encoders: encoders, playout: playoutIntegration, clock: clock}
}

func (coordinator *Coordinator) Failover(event model.LiveEvent, profile string) (FailoverState, error) {
	coordinator.audio.Prepare(event.ID)
	item := coordinator.state.Begin(event.ID, coordinator.clock())
	lease, err := coordinator.encoders.ReserveBackup(event.ID, profile)
	if err != nil {
		return item, err
	}
	if !coordinator.state.AttachLease(event.ID, lease) {
		coordinator.encoders.Release(lease)
		return item, fmt.Errorf("backup lease owner does not match event")
	}
	if err := coordinator.encoders.Start(lease); err != nil {
		return item, err
	}
	if err := coordinator.audio.RetirePrimary(event.ID); err != nil {
		return item, err
	}
	if err := coordinator.audio.ConnectBackup(event.ID); err != nil {
		return item, err
	}
	if !coordinator.state.Complete(event.ID, coordinator.clock()) {
		return item, fmt.Errorf("failover did not retain its encoder lease")
	}
	return coordinator.state.GetOrZero(event.ID), nil
}

func (coordinator *Coordinator) Schedule(ctx context.Context, event model.LiveEvent, profile string, delay time.Duration) <-chan error {
	return coordinator.playout.StartWorker(ctx, event, delay, func(workerContext context.Context) error {
		if err := workerContext.Err(); err != nil {
			return err
		}
		_, err := coordinator.Failover(event, profile)
		return err
	})
}

func (coordinator *Coordinator) Cancel(eventID model.EventID) error {
	coordinator.state.Cancel(eventID)
	return coordinator.playout.Cancel(eventID)
}

func (coordinator *Coordinator) Failback(event model.LiveEvent, primaryReady func() bool) (FailoverState, error) {
	item, tracked := coordinator.state.Get(event.ID)
	if tracked && item.Status != "active" {
		return item, fmt.Errorf("failover for event %s is %s, not active", event.ID, item.Status)
	}
	path, ok := coordinator.audio.Path(event.ID)
	if !ok || !path.BackupLive() {
		return item, fmt.Errorf("event %s is not carrying backup media", event.ID)
	}
	if !primaryReady() {
		return item, fmt.Errorf("primary source is not locked and continuous")
	}
	if err := coordinator.audio.PreparePrimary(event.ID); err != nil {
		return item, err
	}
	if !primaryReady() {
		if err := coordinator.audio.RetirePrimary(event.ID); err != nil {
			return item, err
		}
		return item, fmt.Errorf("%w: primary signal dropped again; backup path retained", ErrFailbackAborted)
	}
	if err := coordinator.audio.ReleaseBackup(event.ID); err != nil {
		return item, err
	}
	if !tracked {
		return FailoverState{}, nil
	}
	if !coordinator.state.Restored(event.ID, coordinator.clock()) {
		return item, fmt.Errorf("failback did not retain its failover record")
	}
	restored := coordinator.state.GetOrZero(event.ID)
	if restored.Lease.ID.Valid() {
		if err := coordinator.encoders.Release(restored.Lease); err != nil {
			return restored, fmt.Errorf("backup encoder lease was not released: %w", err)
		}
	}
	return restored, nil
}

func (coordinator *Coordinator) Status(eventID model.EventID) (FailoverState, bool) {
	return coordinator.state.Get(eventID)
}
