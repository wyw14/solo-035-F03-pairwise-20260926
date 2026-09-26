package audio

import (
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

func testClock() func() time.Time {
	return func() time.Time { return time.Unix(100, 0).UTC() }
}

func TestFailbackPreparesPrimaryThenReleasesBackup(t *testing.T) {
	state := NewState()
	switcher := NewSwitch(state, testClock())
	eventID := model.NewEventID()
	switcher.Prepare(eventID)
	if err := switcher.Failover(eventID); err != nil {
		t.Fatal(err)
	}
	path, _ := state.Path(eventID)
	if path.Active() != "backup" {
		t.Fatalf("active = %s after failover", path.Active())
	}
	if err := switcher.PreparePrimary(eventID); err != nil {
		t.Fatal(err)
	}
	path, _ = state.Path(eventID)
	if path.Active() != "both" {
		t.Fatalf("active = %s, want both during make-before-break", path.Active())
	}
	if err := switcher.ReleaseBackup(eventID); err != nil {
		t.Fatal(err)
	}
	path, _ = state.Path(eventID)
	if path.Active() != "primary" {
		t.Fatalf("active = %s after failback", path.Active())
	}
	joined := strings.Join(path.Sequence, ",")
	for _, step := range []string{"prepare-primary-media", "release-backup-media"} {
		if !strings.Contains(joined, step) {
			t.Fatalf("sequence %v missing %s", path.Sequence, step)
		}
	}
}

func TestPreparePrimaryRequiresBackupCarryingProgram(t *testing.T) {
	state := NewState()
	switcher := NewSwitch(state, testClock())
	eventID := model.NewEventID()
	switcher.Prepare(eventID)
	if err := switcher.PreparePrimary(eventID); err == nil {
		t.Fatal("expected prepare to refuse while backup is not carrying the program")
	}
}

func TestReleaseBackupRequiresPrimaryLive(t *testing.T) {
	state := NewState()
	switcher := NewSwitch(state, testClock())
	eventID := model.NewEventID()
	switcher.Prepare(eventID)
	if err := switcher.Failover(eventID); err != nil {
		t.Fatal(err)
	}
	if err := switcher.ReleaseBackup(eventID); err == nil {
		t.Fatal("expected release to refuse before primary is prepared")
	}
	path, _ := state.Path(eventID)
	if path.Active() != "backup" {
		t.Fatalf("active = %s, want backup retained", path.Active())
	}
}

func TestSwitchFailbackOneShot(t *testing.T) {
	state := NewState()
	switcher := NewSwitch(state, testClock())
	eventID := model.NewEventID()
	switcher.Prepare(eventID)
	if err := switcher.Failover(eventID); err != nil {
		t.Fatal(err)
	}
	if err := switcher.Failback(eventID); err != nil {
		t.Fatal(err)
	}
	path, _ := state.Path(eventID)
	if path.Active() != "primary" {
		t.Fatalf("active = %s after failback", path.Active())
	}
}
