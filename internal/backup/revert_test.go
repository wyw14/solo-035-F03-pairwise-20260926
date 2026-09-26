package backup

import (
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/audio"
	"github.com/wyw14/solo-035/internal/encoder"
	"github.com/wyw14/solo-035/internal/model"
	"github.com/wyw14/solo-035/internal/playout"
	"github.com/wyw14/solo-035/internal/source"
	"github.com/wyw14/solo-035/internal/syncer"
)

type revertHarness struct {
	event    model.LiveEvent
	primary  model.SourceID
	base     time.Time
	audio    *audio.Switch
	sources  *source.Coordinator
	sync     *syncer.Coordinator
	encoders *encoder.Coordinator
	backup   *Coordinator
}

func newRevertHarness(t *testing.T) *revertHarness {
	t.Helper()
	base := time.Unix(1000, 0).UTC()
	clock := func() time.Time { return base }

	audioSwitch := audio.NewSwitch(audio.NewState(), clock)
	encoderCoord := encoder.NewCoordinator(encoder.NewState("encoder-primary-1", "encoder-backup-1"), clock)
	playoutFlow := playout.NewIntegration(playout.NewState())
	sourceCoord := source.NewCoordinator(source.NewState(), clock)
	syncCoord := syncer.NewCoordinator(syncer.NewState())
	coordinator := NewCoordinator(NewState(), audioSwitch, encoderCoord, playoutFlow, sourceCoord, syncCoord, clock)

	event := model.LiveEvent{ID: model.NewEventID(), Name: "Evening News", Phase: model.PhaseOnAir, Revision: model.NewRevisionID(), CommandEpoch: 1}
	return &revertHarness{
		event:    event,
		primary:  model.NewSourceID(),
		base:     base,
		audio:    audioSwitch,
		sources:  sourceCoord,
		sync:     syncCoord,
		encoders: encoderCoord,
		backup:   coordinator,
	}
}

// failover puts the event onto the backup path, exactly as production does.
func (h *revertHarness) failover(t *testing.T) model.EncoderLease {
	t.Helper()
	item, err := h.backup.Failover(h.event, "1080p50-backup")
	if err != nil {
		t.Fatalf("failover: %v", err)
	}
	return item.Lease
}

// recoverPrimary simulates the recovered primary: fresh source generation,
// full lock and a window of continuous frames.
func (h *revertHarness) recoverPrimary(t *testing.T) {
	t.Helper()
	status := h.sources.Switch(h.primary)
	if err := h.sources.Confirm(h.primary, status.CurrentCommand, true, true); err != nil {
		t.Fatalf("confirm primary lock: %v", err)
	}
	h.sync.Open(h.event.ID, 3)
	generation := status.Generation
	for sequence := int64(1); sequence <= 3; sequence++ {
		frame := model.Frame{
			Source:           h.primary,
			SourceGeneration: generation,
			Sequence:         sequence,
			Timecode:         1000 + sequence,
			CapturedAt:       h.base.Add(time.Duration(sequence) * 20 * time.Millisecond),
			VideoLocked:      true,
			AudioLocked:      true,
		}
		if _, err := h.sync.Observe(h.event.ID, frame); err != nil {
			t.Fatalf("observe frame: %v", err)
		}
	}
}

// dropPrimary simulates the recovered signal falling away again: a new
// source generation with neither lock held.
func (h *revertHarness) dropPrimary() {
	h.sources.Switch(h.primary)
}

func (h *revertHarness) assertBackupOnly(t *testing.T, label string) {
	t.Helper()
	path, _ := h.audio.State().Path(h.event.ID)
	if !path.BackupVideoLive || !path.BackupAudioLive || path.PrimaryVideoLive || path.PrimaryAudioLive {
		t.Fatalf("%s: backup must be the sole live path, got %+v", label, path)
	}
}

func TestRevertFailsWithoutActiveFailover(t *testing.T) {
	harness := newRevertHarness(t)
	harness.recoverPrimary(t)
	if _, err := harness.backup.Revert(harness.event, harness.primary); err == nil {
		t.Fatal("expected revert to be refused when backup is not active")
	}
}

func TestRevertRequiresLockedContinuousPrimary(t *testing.T) {
	harness := newRevertHarness(t)
	harness.failover(t)

	// Primary is neither locked nor sending continuous pictures.
	if _, err := harness.backup.Revert(harness.event, harness.primary); err == nil {
		t.Fatal("expected revert to be refused without a recovered primary")
	}
	harness.assertBackupOnly(t, "after refusal")

	// Locked but with no continuous frame window: still refused.
	status := harness.sources.Switch(harness.primary)
	if err := harness.sources.Confirm(harness.primary, status.CurrentCommand, true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.backup.Revert(harness.event, harness.primary); err == nil {
		t.Fatal("expected revert to be refused without continuous pictures")
	}
	harness.assertBackupOnly(t, "after lock-only refusal")
}

func TestRevertCompletesAndReleasesBackup(t *testing.T) {
	harness := newRevertHarness(t)
	lease := harness.failover(t)
	harness.recoverPrimary(t)

	result, err := harness.backup.Revert(harness.event, harness.primary)
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	if result.Stage != RevertReverted {
		t.Fatalf("stage = %q, want reverted", result.Stage)
	}
	path := result.Path
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive || path.BackupVideoLive || path.BackupAudioLive {
		t.Fatalf("primary must be the sole live path after revert: %+v", path)
	}
	if len(path.Usage) != 3 {
		t.Fatalf("usage entries = %d, want 3", len(path.Usage))
	}
	if path.Usage[0].Route != "primary" || path.Usage[0].EndedAt == nil {
		t.Fatalf("original primary usage must be closed: %+v", path.Usage[0])
	}
	if path.Usage[1].Route != "backup" || path.Usage[1].Reason != "failover" || path.Usage[1].EndedAt == nil {
		t.Fatalf("backup usage must be closed: %+v", path.Usage[1])
	}
	if path.Usage[2].Route != "primary" || path.Usage[2].Reason != "switchback" || path.Usage[2].EndedAt != nil {
		t.Fatalf("switchback primary usage must be open: %+v", path.Usage[2])
	}
	for _, resource := range harness.encoders.Equipment() {
		if resource.ID == lease.EncoderID && resource.Lease.Owner != "" {
			t.Fatalf("backup encoder %s lease must be released, owner = %s", lease.EncoderID, resource.Lease.Owner)
		}
	}
	item, _ := harness.backup.Status(harness.event.ID)
	if item.Status != "reverted" {
		t.Fatalf("failover status = %q, want reverted", item.Status)
	}
}

func TestSignalDroppingMidRevertKeepsBackupPlayable(t *testing.T) {
	harness := newRevertHarness(t)
	harness.failover(t)
	harness.recoverPrimary(t)

	// Primary video is prepared under a healthy primary...
	harness.backup.state.BeginRevert(harness.event.ID, harness.base)
	if err := harness.audio.PreparePrimaryVideo(harness.event.ID); err != nil {
		t.Fatalf("prepare primary video: %v", err)
	}
	// ...then the signal drops again. The post-video gate must fail.
	harness.dropPrimary()
	healthy, reason := harness.backup.primaryHealthy(harness.event.ID, harness.primary)
	if healthy {
		t.Fatal("primary gate must fail after the signal drops")
	}
	if reason == "" {
		t.Fatal("gate failure must carry a reason")
	}

	// Aborting retires the partially prepared primary without touching backup.
	result := harness.backup.abortRevert(harness.event.ID, RevertPrepareVideo, reason)
	if result.Stage != RevertAborted {
		t.Fatalf("stage = %q, want aborted", result.Stage)
	}
	harness.assertBackupOnly(t, "after mid-revert abort")
	item, _ := harness.backup.Status(harness.event.ID)
	if item.Status != "active" || item.RevertStage != "aborted" {
		t.Fatalf("unexpected failover state after abort: status=%q stage=%q", item.Status, item.RevertStage)
	}
	path, _ := harness.audio.State().Path(harness.event.ID)
	if len(path.Usage) != 2 || path.Usage[1].EndedAt != nil {
		t.Fatalf("backup usage must remain open after abort: %+v", path.Usage)
	}
	foundAbort := false
	for _, step := range path.Sequence {
		if step == "abort-switchback:prepare-primary-video:primary source is not locked" {
			foundAbort = true
		}
	}
	if !foundAbort {
		t.Fatalf("abort must be recorded in path sequence: %v", path.Sequence)
	}

	// Once the primary recovers, a retry completes cleanly.
	harness.recoverPrimary(t)
	result, err := harness.backup.Revert(harness.event, harness.primary)
	if err != nil {
		t.Fatalf("retry revert: %v", err)
	}
	if result.Stage != RevertReverted {
		t.Fatalf("retry stage = %q, want reverted", result.Stage)
	}
	if !result.Path.PrimaryVideoLive || !result.Path.PrimaryAudioLive || result.Path.BackupVideoLive || result.Path.BackupAudioLive {
		t.Fatalf("retry must leave primary as the sole live path: %+v", result.Path)
	}
}
