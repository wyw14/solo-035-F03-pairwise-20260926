package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/audio"
	"github.com/wyw14/solo-035/internal/encoder"
	"github.com/wyw14/solo-035/internal/model"
	"github.com/wyw14/solo-035/internal/playout"
	"github.com/wyw14/solo-035/internal/source"
	"github.com/wyw14/solo-035/internal/syncer"
)

type Coordinator struct {
	state    *State
	audio    *audio.Switch
	encoders *encoder.Coordinator
	playout  *playout.Integration
	sources  *source.Coordinator
	sync     *syncer.Coordinator
	clock    func() time.Time
}

func NewCoordinator(
	state *State,
	audioSwitch *audio.Switch,
	encoders *encoder.Coordinator,
	playoutIntegration *playout.Integration,
	sourceFlow *source.Coordinator,
	syncFlow *syncer.Coordinator,
	clock func() time.Time,
) *Coordinator {
	return &Coordinator{
		state:    state,
		audio:    audioSwitch,
		encoders: encoders,
		playout:  playoutIntegration,
		sources:  sourceFlow,
		sync:     syncFlow,
		clock:    clock,
	}
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

func (coordinator *Coordinator) Status(eventID model.EventID) (FailoverState, bool) {
	return coordinator.state.Get(eventID)
}
