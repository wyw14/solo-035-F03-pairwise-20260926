package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/audio"
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

type FailbackResult struct {
	EventID    model.EventID         `json:"event_id"`
	Restored   bool                  `json:"restored"`
	ActivePath string                `json:"active_path"`
	Failover   *backup.FailoverState `json:"failover,omitempty"`
	Audio      audio.PathState       `json:"audio"`
}

func (coordinator *Coordinator) Failback(eventID model.EventID, sourceID model.SourceID) (FailbackResult, error) {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return FailbackResult{}, fmt.Errorf("event %s not found", eventID)
	}
	primaryReady := func() bool {
		if !coordinator.runtime.SourceFlow.Ready(sourceID) {
			return false
		}
		status, ok := coordinator.runtime.Sync.Status(eventID)
		return ok && status.Stable && status.Source == sourceID
	}
	state, err := coordinator.runtime.Backup.Failback(event, primaryReady)
	aborted := errors.Is(err, backup.ErrFailbackAborted)
	if err != nil && !aborted {
		return FailbackResult{}, err
	}
	result := FailbackResult{EventID: eventID, Restored: !aborted}
	if state.Status != "" {
		result.Failover = &state
	}
	result.Audio, _ = coordinator.runtime.AudioState.Path(eventID)
	result.ActivePath = result.Audio.Active()
	details := map[string]any{"restored": result.Restored, "source": sourceID}
	if state.Status != "" {
		details["failover_status"] = state.Status
	}
	if recordErr := coordinator.recordMediaPath(eventID, "audio-failback", details); recordErr != nil {
		return result, recordErr
	}
	return result, nil
}

func (coordinator *Coordinator) recordMediaPath(eventID model.EventID, trigger string, details map[string]any) error {
	path, ok := coordinator.runtime.AudioState.Path(eventID)
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	payload := map[string]any{
		"trigger":      trigger,
		"active_path":  path.Active(),
		"primary_live": path.PrimaryLive(),
		"backup_live":  path.BackupLive(),
	}
	for key, value := range details {
		payload[key] = value
	}
	_, err := coordinator.runtime.Journal.Persist(eventID, "media-path", "", 0, payload)
	return err
}
