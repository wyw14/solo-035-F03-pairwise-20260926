package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/backup"
	"github.com/wyw14/solo-035/internal/caption"
	"github.com/wyw14/solo-035/internal/model"
)

type Coordinator struct {
	runtime *Runtime
}

func NewCoordinator(runtime *Runtime) *Coordinator {
	return &Coordinator{runtime: runtime}
}

func (coordinator *Coordinator) CreateEvent(name string) (model.LiveEvent, error) {
	event, err := coordinator.runtime.Events.Create(name, coordinator.runtime.Clock())
	if err != nil {
		return model.LiveEvent{}, err
	}
	if _, err := coordinator.runtime.EventFlow.Arm(event.ID); err != nil {
		return model.LiveEvent{}, err
	}
	return coordinator.runtime.Events.GetOrZero(event.ID), nil
}

func (coordinator *Coordinator) PrepareSignal(eventID model.EventID, sourceID model.SourceID) (model.SignalStatus, error) {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return model.SignalStatus{}, fmt.Errorf("event %s not found", eventID)
	}
	status := coordinator.runtime.SourceFlow.Switch(sourceID)
	event = event.NewSource(coordinator.runtime.Clock())
	event.SourceGeneration = status.Generation
	if err := coordinator.runtime.Events.Update(event); err != nil {
		return model.SignalStatus{}, err
	}
	coordinator.runtime.Quality.Reset(eventID)
	if err := coordinator.runtime.Sync.Open(eventID, 3); err != nil {
		return model.SignalStatus{}, err
	}
	return status, nil
}

func (coordinator *Coordinator) ConfirmSignal(sourceID model.SourceID, command model.CommandID) error {
	if err := coordinator.runtime.SourceFlow.Confirm(sourceID, command, true, true); err != nil {
		return err
	}
	if !coordinator.runtime.SourceFlow.Ready(sourceID) {
		return fmt.Errorf("source %s did not retain its current lock", sourceID)
	}
	return nil
}

func (coordinator *Coordinator) PushFrame(eventID model.EventID, frame model.Frame) error {
	received, err := coordinator.runtime.Receiver.Push(frame.Source, frame.SourceGeneration, frame.Sequence, frame.Timecode, frame.CapturedAt)
	if err != nil {
		return err
	}
	received.VideoLocked = frame.VideoLocked
	received.AudioLocked = frame.AudioLocked
	_, err = coordinator.runtime.Sync.Observe(eventID, received)
	return err
}

func (coordinator *Coordinator) ConfigureProgram(eventID model.EventID, sourceID model.SourceID) error {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	lease, err := coordinator.runtime.EncoderFlow.ReservePrimary(eventID, "1080p50")
	if err != nil {
		return err
	}
	if err := coordinator.runtime.EncoderFlow.Start(lease); err != nil {
		return err
	}
	program, err := coordinator.runtime.Mux.Configure(event, lease, 8000)
	if err != nil {
		return err
	}
	if err := coordinator.runtime.Mux.Activate(eventID, program.Revision); err != nil {
		return err
	}
	return coordinator.runtime.Playout.Stage(event, sourceID, lease.Fence, "current-program")
}

func (coordinator *Coordinator) EstablishInterlocks(eventID model.EventID) error {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	waveA, _, err := coordinator.runtime.AudioMonitor.Measure("probe-a", -18, coordinator.runtime.Clock())
	if err != nil {
		return err
	}
	waveB, _, err := coordinator.runtime.AudioMonitor.Measure("probe-b", -17, coordinator.runtime.Clock())
	if err != nil {
		return err
	}
	if err := coordinator.runtime.Evaluator.Accept(waveA); err != nil {
		return err
	}
	if err := coordinator.runtime.Evaluator.Accept(waveB); err != nil {
		return err
	}
	if err := coordinator.runtime.Quality.Record(eventID, waveA); err != nil {
		return err
	}
	if err := coordinator.runtime.Quality.Record(eventID, waveB); err != nil {
		return err
	}
	if err := coordinator.runtime.Continuity.Accept(eventID, caption.Cue{Revision: event.Revision, Sequence: 1, Text: "ON AIR"}); err != nil {
		return err
	}
	coordinator.runtime.CaptionFlow.Encode(eventID, event.Revision, "ON AIR")
	return nil
}

func (coordinator *Coordinator) CommitOnAir(eventID model.EventID, fence uint64) error {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	if !coordinator.runtime.Sync.Ready(eventID) {
		return fmt.Errorf("frame synchronization is not stable")
	}
	if err := coordinator.runtime.CaptionFlow.Ready(eventID, event.Revision); err != nil {
		return err
	}
	if _, err := coordinator.runtime.Quality.Permit(eventID, true, true); err != nil {
		return err
	}
	if err := coordinator.runtime.Playout.CommitOnAir(event, fence, map[string]any{"offset_ms": 0, "source_generation": event.SourceGeneration}); err != nil {
		return err
	}
	_, err := coordinator.runtime.EventFlow.MarkOnAir(eventID)
	return err
}

func (coordinator *Coordinator) ScheduleFailover(ctx context.Context, eventID model.EventID, delay time.Duration) (<-chan error, error) {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return nil, fmt.Errorf("event %s not found", eventID)
	}
	return coordinator.runtime.Backup.Schedule(ctx, event, "1080p50-backup", delay), nil
}

func (coordinator *Coordinator) CancelFailover(eventID model.EventID) error {
	if _, err := coordinator.runtime.EventFlow.CancelCommand(eventID); err != nil {
		return err
	}
	return coordinator.runtime.Backup.Cancel(eventID)
}

// RevertSwitchback returns a failed-over event to the recovered primary.
// It refuses unless the primary source is locked and its pictures are
// continuous; if the signal drops at any gate the backup path stays on air.
func (coordinator *Coordinator) RevertSwitchback(eventID model.EventID, sourceID model.SourceID) (backup.RevertResult, error) {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return backup.RevertResult{}, fmt.Errorf("event %s not found", eventID)
	}
	if !sourceID.Valid() {
		if status, known := coordinator.runtime.Sync.Status(eventID); known {
			sourceID = status.Source
		}
	}
	result, err := coordinator.runtime.Backup.Revert(event, sourceID)
	if err != nil {
		return result, err
	}
	if _, err := coordinator.runtime.EventFlow.MarkOnAir(eventID); err != nil {
		return result, fmt.Errorf("primary media is live but event phase failed to return on air: %w", err)
	}
	return result, nil
}

func (coordinator *Coordinator) ScheduleRevert(ctx context.Context, eventID model.EventID, sourceID model.SourceID, delay time.Duration) (<-chan error, error) {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return nil, fmt.Errorf("event %s not found", eventID)
	}
	if !sourceID.Valid() {
		if status, known := coordinator.runtime.Sync.Status(eventID); known {
			sourceID = status.Source
		}
	}
	return coordinator.runtime.Backup.ScheduleRevert(ctx, event, sourceID, delay), nil
}
