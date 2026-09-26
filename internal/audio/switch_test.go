package audio

import (
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

func testClock() func() time.Time {
	base := time.Unix(1000, 0).UTC()
	return func() time.Time {
		base = base.Add(time.Second)
		return base
	}
}

func failoverToBackup(t *testing.T, switcher *Switch, eventID model.EventID) {
	t.Helper()
	switcher.Prepare(eventID)
	if err := switcher.RetirePrimary(eventID); err != nil {
		t.Fatalf("retire primary: %v", err)
	}
	if err := switcher.ConnectBackup(eventID); err != nil {
		t.Fatalf("connect backup: %v", err)
	}
}

func TestFailoverThenSwitchbackLifecycle(t *testing.T) {
	state := NewState()
	clock := testClock()
	switcher := NewSwitch(state, clock)
	eventID := model.NewEventID()

	failoverToBackup(t, switcher, eventID)
	path, _ := state.Path(eventID)
	if path.PrimaryVideoLive || path.PrimaryAudioLive || !path.BackupVideoLive || !path.BackupAudioLive {
		t.Fatalf("unexpected path after failover: %+v", path)
	}

	// Primary video is prepared while backup still carries the program.
	if err := switcher.PreparePrimaryVideo(eventID); err != nil {
		t.Fatalf("prepare primary video: %v", err)
	}
	path, _ = state.Path(eventID)
	if !path.PrimaryVideoLive || path.PrimaryAudioLive || !path.BackupVideoLive || !path.BackupAudioLive {
		t.Fatalf("backup must stay on air while primary video is prepared: %+v", path)
	}

	// Audio follows video; backup audio is still live until the cut completes.
	if err := switcher.PreparePrimaryAudio(eventID); err != nil {
		t.Fatalf("prepare primary audio: %v", err)
	}
	path, _ = state.Path(eventID)
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive || !path.BackupVideoLive || !path.BackupAudioLive {
		t.Fatalf("both paths must be live at the cut: %+v", path)
	}

	// Backup is released only after primary fully carries the program.
	if err := switcher.ReleaseBackup(eventID); err != nil {
		t.Fatalf("release backup: %v", err)
	}
	path, _ = state.Path(eventID)
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive || path.BackupVideoLive || path.BackupAudioLive {
		t.Fatalf("primary must be the sole live path after switchback: %+v", path)
	}

	if len(path.Usage) != 3 {
		t.Fatalf("usage entries = %d, want 3: %+v", len(path.Usage), path.Usage)
	}
	first, backup, second := path.Usage[0], path.Usage[1], path.Usage[2]
	if first.Route != "primary" || first.Reason != "primary-open" || first.EndedAt == nil {
		t.Fatalf("unexpected first usage: %+v", first)
	}
	if backup.Route != "backup" || backup.Reason != "failover" || backup.EndedAt == nil {
		t.Fatalf("unexpected backup usage: %+v", backup)
	}
	if second.Route != "primary" || second.Reason != "switchback" || second.EndedAt != nil {
		t.Fatalf("unexpected switchback usage: %+v", second)
	}
}

func TestSwitchbackGuards(t *testing.T) {
	state := NewState()
	switcher := NewSwitch(state, testClock())
	eventID := model.NewEventID()
	switcher.Prepare(eventID)

	// Primary media cannot be "prepared" while it is already on air.
	if err := switcher.PreparePrimaryVideo(eventID); err == nil {
		t.Fatal("expected primary preparation to be refused before failover")
	}
	// Backup cannot be released while primary is not live.
	if err := switcher.ReleaseBackup(eventID); err == nil {
		t.Fatal("expected backup release to be refused while primary is off air")
	}

	failoverToBackup(t, switcher, eventID)
	// Audio cannot precede video during preparation.
	if err := switcher.PreparePrimaryAudio(eventID); err == nil {
		t.Fatal("expected primary audio preparation to be refused before video")
	}
	if err := switcher.PreparePrimaryVideo(eventID); err != nil {
		t.Fatalf("prepare primary video: %v", err)
	}
	// Backup still cannot release until primary audio is live too.
	if err := switcher.ReleaseBackup(eventID); err == nil {
		t.Fatal("expected backup release to be refused with only primary video live")
	}
}

func TestAbortSwitchbackKeepsBackupPlayable(t *testing.T) {
	state := NewState()
	switcher := NewSwitch(state, testClock())
	eventID := model.NewEventID()
	failoverToBackup(t, switcher, eventID)

	if err := switcher.PreparePrimaryVideo(eventID); err != nil {
		t.Fatalf("prepare primary video: %v", err)
	}
	if err := switcher.PreparePrimaryAudio(eventID); err != nil {
		t.Fatalf("prepare primary audio: %v", err)
	}

	// Signal drops mid-switchback: abort must retire prepared primary media
	// without touching the backup path.
	if err := switcher.AbortSwitchback(eventID, "primary continuity lost"); err != nil {
		t.Fatalf("abort: %v", err)
	}
	path, _ := state.Path(eventID)
	if path.PrimaryVideoLive || path.PrimaryAudioLive {
		t.Fatalf("primary media must be retired after abort: %+v", path)
	}
	if !path.BackupVideoLive || !path.BackupAudioLive {
		t.Fatalf("backup must remain the playable path after abort: %+v", path)
	}

	// A later retry can complete the switchback once primary is healthy again.
	if err := switcher.PreparePrimaryVideo(eventID); err != nil {
		t.Fatalf("retry prepare primary video: %v", err)
	}
	if err := switcher.PreparePrimaryAudio(eventID); err != nil {
		t.Fatalf("retry prepare primary audio: %v", err)
	}
	if err := switcher.ReleaseBackup(eventID); err != nil {
		t.Fatalf("retry release backup: %v", err)
	}
	path, _ = state.Path(eventID)
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive || path.BackupVideoLive || path.BackupAudioLive {
		t.Fatalf("retry must leave primary as the sole live path: %+v", path)
	}
}

func TestUsageRecordsOnlyPathsThatCarriedProgram(t *testing.T) {
	state := NewState()
	switcher := NewSwitch(state, testClock())
	eventID := model.NewEventID()
	failoverToBackup(t, switcher, eventID)

	// Primary video prepared, then aborted before audio cuts: it never
	// carried program audio, so no new primary usage segment is opened.
	if err := switcher.PreparePrimaryVideo(eventID); err != nil {
		t.Fatalf("prepare primary video: %v", err)
	}
	if err := switcher.AbortSwitchback(eventID, "signal dropped"); err != nil {
		t.Fatalf("abort: %v", err)
	}
	path, _ := state.Path(eventID)
	if len(path.Usage) != 2 {
		t.Fatalf("usage entries = %d, want 2 (original primary + backup): %+v", len(path.Usage), path.Usage)
	}
	backupUsage := path.Usage[1]
	if backupUsage.Route != "backup" || backupUsage.EndedAt != nil {
		t.Fatalf("backup usage must remain open after aborted switchback: %+v", backupUsage)
	}
}
