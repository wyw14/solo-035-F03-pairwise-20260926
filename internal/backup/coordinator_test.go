package backup

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/audio"
	"github.com/wyw14/solo-035/internal/encoder"
	"github.com/wyw14/solo-035/internal/model"
	"github.com/wyw14/solo-035/internal/playout"
)

func newTestCoordinator() (*Coordinator, *audio.State, *encoder.State) {
	clock := func() time.Time { return time.Unix(100, 0).UTC() }
	audioState := audio.NewState()
	switcher := audio.NewSwitch(audioState, clock)
	encoderState := encoder.NewState("encoder-primary-1", "encoder-backup-1")
	encoders := encoder.NewCoordinator(encoderState, clock)
	integration := playout.NewIntegration(playout.NewState())
	return NewCoordinator(NewState(), switcher, encoders, integration, clock), audioState, encoderState
}

func onAirEvent() model.LiveEvent {
	return model.LiveEvent{ID: model.NewEventID(), Phase: model.PhaseOnAir}
}

func TestFailbackRestoresPrimaryAndReleasesBackup(t *testing.T) {
	coordinator, audioState, encoderState := newTestCoordinator()
	event := onAirEvent()
	if _, err := coordinator.Failover(event, "1080p50-backup"); err != nil {
		t.Fatal(err)
	}
	state, err := coordinator.Failback(event, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "restored" || state.RestoredAt == nil {
		t.Fatalf("status = %s restoredAt = %v", state.Status, state.RestoredAt)
	}
	path, _ := audioState.Path(event.ID)
	if path.Active() != "primary" {
		t.Fatalf("active = %s, want primary restored", path.Active())
	}
	if owner := encoderState.Owner(state.Lease.EncoderID); owner != "" {
		t.Fatalf("backup encoder lease still owned by %s", owner)
	}
}

func TestFailbackAbortedWhenPrimaryDropsMidway(t *testing.T) {
	coordinator, audioState, encoderState := newTestCoordinator()
	event := onAirEvent()
	if _, err := coordinator.Failover(event, "1080p50-backup"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	probe := func() bool { calls++; return calls == 1 }
	state, err := coordinator.Failback(event, probe)
	if !errors.Is(err, ErrFailbackAborted) {
		t.Fatalf("err = %v, want ErrFailbackAborted", err)
	}
	if state.Status != "active" {
		t.Fatalf("status = %s, want backup still active", state.Status)
	}
	path, _ := audioState.Path(event.ID)
	if path.Active() != "backup" {
		t.Fatalf("active = %s, want playable backup path retained", path.Active())
	}
	if owner := encoderState.Owner(state.Lease.EncoderID); owner != event.ID {
		t.Fatalf("backup lease owner = %s, want event retained", owner)
	}
	state, err = coordinator.Failback(event, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "restored" {
		t.Fatalf("status = %s after retry", state.Status)
	}
	if path, _ := audioState.Path(event.ID); path.Active() != "primary" {
		t.Fatalf("active = %s after retry", path.Active())
	}
}

func TestFailbackRequiresBackupPath(t *testing.T) {
	coordinator, _, _ := newTestCoordinator()
	if _, err := coordinator.Failback(onAirEvent(), func() bool { return true }); err == nil {
		t.Fatal("expected failback to refuse when backup media is not carrying the program")
	}
}

func TestFailbackRefusesRepeatedRestore(t *testing.T) {
	coordinator, _, _ := newTestCoordinator()
	event := onAirEvent()
	if _, err := coordinator.Failover(event, "1080p50-backup"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Failback(event, func() bool { return true }); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Failback(event, func() bool { return true }); err == nil {
		t.Fatal("expected a second failback to refuse once primary is restored")
	}
}

func TestFailbackAfterAudioOnlyFailover(t *testing.T) {
	coordinator, audioState, _ := newTestCoordinator()
	event := onAirEvent()
	coordinator.audio.Prepare(event.ID)
	if err := coordinator.audio.Failover(event.ID); err != nil {
		t.Fatal(err)
	}
	state, err := coordinator.Failback(event, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "" {
		t.Fatalf("status = %s, want no failover record for audio-only switch", state.Status)
	}
	if path, _ := audioState.Path(event.ID); path.Active() != "primary" {
		t.Fatalf("active = %s, want primary restored", path.Active())
	}
}

func TestFailbackRequiresLockedPrimary(t *testing.T) {
	coordinator, audioState, _ := newTestCoordinator()
	event := onAirEvent()
	if _, err := coordinator.Failover(event, "1080p50-backup"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Failback(event, func() bool { return false }); err == nil {
		t.Fatal("expected failback to refuse while primary is not ready")
	}
	path, _ := audioState.Path(event.ID)
	if path.Active() != "backup" {
		t.Fatalf("active = %s, want backup retained", path.Active())
	}
	if len(path.Sequence) != 4 {
		t.Fatalf("sequence = %v, want no failback steps", path.Sequence)
	}
}
