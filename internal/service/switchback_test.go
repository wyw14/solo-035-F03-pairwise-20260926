package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/backup"
	"github.com/wyw14/solo-035/internal/model"
)

// pushFrames feeds a continuous locked frame window for a signal status
// returned by PrepareSignal.
func pushFrames(t *testing.T, coordinator *Coordinator, eventID model.EventID, signal model.SignalStatus, firstTimecode int64) {
	t.Helper()
	base := coordinator.runtime.Clock().UTC()
	for sequence := int64(1); sequence <= 3; sequence++ {
		frame := model.Frame{
			Source:           signal.Source,
			SourceGeneration: signal.Generation,
			Sequence:         sequence,
			Timecode:         firstTimecode + int64(sequence),
			CapturedAt:       base.Add(time.Duration(sequence) * 20 * time.Millisecond),
			VideoLocked:      true,
			AudioLocked:      true,
		}
		if err := coordinator.PushFrame(eventID, frame); err != nil {
			t.Fatalf("push frame: %v", err)
		}
	}
}

// losePrimary simulates the primary dropping: a new unlocked source
// generation followed by one unlocked frame that breaks continuity.
func losePrimary(t *testing.T, coordinator *Coordinator, eventID model.EventID, sourceID model.SourceID, timecode int64) {
	t.Helper()
	signal := coordinator.runtime.SourceFlow.Switch(sourceID)
	frame := model.Frame{
		Source:           sourceID,
		SourceGeneration: signal.Generation,
		Sequence:         99,
		Timecode:         timecode,
		CapturedAt:       coordinator.runtime.Clock().UTC(),
		VideoLocked:      false,
		AudioLocked:      false,
	}
	if _, err := coordinator.runtime.Sync.Observe(eventID, frame); err != nil {
		t.Fatalf("observe dropped frame: %v", err)
	}
}

func TestSwitchbackFlowEndToEnd(t *testing.T) {
	runtime, err := NewRuntime(t.TempDir(), func() time.Time { return time.Unix(100, 0).UTC() })
	if err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(runtime); err != nil {
		t.Fatal(err)
	}
	coordinator := NewCoordinator(runtime)
	event := runtime.Events.List()[0]
	primarySource, _ := runtime.Sync.Status(event.ID)

	// Primary fails: scheduled failover moves the program onto backup.
	if err := <-runtime.Backup.Schedule(context.Background(), event, "1080p50-backup", 0); err != nil {
		t.Fatalf("failover: %v", err)
	}
	if _, err := runtime.EventFlow.MarkFailedOver(event.ID); err != nil {
		t.Fatal(err)
	}
	path, _ := runtime.AudioState.Path(event.ID)
	if path.PrimaryVideoLive || path.PrimaryAudioLive || !path.BackupVideoLive || !path.BackupAudioLive {
		t.Fatalf("program must be on backup after failover: %+v", path)
	}

	// While the primary is still down, switchback is refused and the
	// backup path stays playable.
	losePrimary(t, coordinator, event.ID, primarySource.Source, 9000)
	if _, err := coordinator.RevertSwitchback(event.ID, primarySource.Source); err == nil {
		t.Fatal("revert must be refused while primary is not recovered")
	}
	path, _ = runtime.AudioState.Path(event.ID)
	if !path.BackupVideoLive || !path.BackupAudioLive || path.PrimaryVideoLive || path.PrimaryAudioLive {
		t.Fatalf("backup must survive a refused switchback: %+v", path)
	}

	// Primary recovers: fresh signal generation, lock and continuous frames.
	signal, err := coordinator.PrepareSignal(event.ID, primarySource.Source)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ConfirmSignal(primarySource.Source, signal.CurrentCommand); err != nil {
		t.Fatal(err)
	}
	pushFrames(t, coordinator, event.ID, signal, 3000)
	if !runtime.Sync.Ready(event.ID) {
		t.Fatal("primary frame window must be stable before revert")
	}

	// Switchback completes: primary carries the program again and the
	// backup encoder is released.
	result, err := coordinator.RevertSwitchback(event.ID, primarySource.Source)
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	if result.Stage != backup.RevertReverted {
		t.Fatalf("stage = %q", result.Stage)
	}
	path, _ = runtime.AudioState.Path(event.ID)
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive || path.BackupVideoLive || path.BackupAudioLive {
		t.Fatalf("primary must be the sole live path after switchback: %+v", path)
	}
	for _, resource := range runtime.EncoderFlow.Equipment() {
		if resource.ID == "encoder-backup-1" && resource.Lease.Owner != "" {
			t.Fatalf("backup encoder must be released, owner = %s", resource.Lease.Owner)
		}
	}
	if len(path.Usage) != 3 {
		t.Fatalf("usage entries = %d, want 3: %+v", len(path.Usage), path.Usage)
	}
	if path.Usage[1].Route != "backup" || path.Usage[1].EndedAt == nil ||
		path.Usage[2].Route != "primary" || path.Usage[2].Reason != "switchback" || path.Usage[2].EndedAt != nil {
		t.Fatalf("unexpected usage ledger: %+v", path.Usage)
	}
	restored, _ := runtime.Events.Get(event.ID)
	if restored.Phase != model.PhaseOnAir {
		t.Fatalf("phase = %q, want on-air", restored.Phase)
	}

	// The reverted state survives a checkpoint round-trip.
	checkpoint := runtime.ExportCheckpoint()
	if len(checkpoint.Backup) != 1 || checkpoint.Backup[0].Status != "reverted" {
		t.Fatalf("checkpoint must record reverted failover: %+v", checkpoint.Backup)
	}
}

func TestRevertControlAction(t *testing.T) {
	runtime, err := NewRuntime(t.TempDir(), func() time.Time { return time.Unix(100, 0).UTC() })
	if err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(runtime); err != nil {
		t.Fatal(err)
	}
	coordinator := NewCoordinator(runtime)
	event := runtime.Events.List()[0]
	primarySource, _ := runtime.Sync.Status(event.ID)
	if err := <-runtime.Backup.Schedule(context.Background(), event, "1080p50-backup", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.EventFlow.MarkFailedOver(event.ID); err != nil {
		t.Fatal(err)
	}
	losePrimary(t, coordinator, event.ID, primarySource.Source, 9000)

	signal, err := coordinator.PrepareSignal(event.ID, primarySource.Source)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ConfirmSignal(primarySource.Source, signal.CurrentCommand); err != nil {
		t.Fatal(err)
	}
	pushFrames(t, coordinator, event.ID, signal, 4000)

	result, err := runtime.ExecuteControl(context.Background(), ControlRequest{
		Action:   "revert-primary",
		EventID:  string(event.ID),
		SourceID: string(primarySource.Source),
	})
	if err != nil {
		t.Fatalf("revert-primary control: %v", err)
	}
	if result.Status != "accepted" {
		t.Fatalf("result status = %q", result.Status)
	}
	revert, ok := result.Value.(backup.RevertResult)
	if !ok || revert.Stage != backup.RevertReverted {
		t.Fatalf("unexpected control result: %#v", result.Value)
	}
	restored, _ := runtime.Events.Get(event.ID)
	if restored.Phase != model.PhaseOnAir {
		t.Fatalf("phase = %q, want on-air", restored.Phase)
	}
	path, _ := runtime.AudioState.Path(event.ID)
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive || path.BackupVideoLive || path.BackupAudioLive {
		t.Fatalf("primary must carry the program after revert-primary: %+v", path)
	}
}
