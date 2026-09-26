package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/audio"
	"github.com/wyw14/solo-035/internal/model"
)

// RevertStage names the switchback phase that was reached.
type RevertStage string

const (
	RevertAborted       RevertStage = "aborted"
	RevertReverted      RevertStage = "reverted"
	RevertVerifyPrimary RevertStage = "verify-primary"
	RevertPrepareVideo  RevertStage = "prepare-primary-video"
	RevertPrepareAudio  RevertStage = "prepare-primary-audio"
	RevertReleaseBackup RevertStage = "release-backup"
)

// RevertResult reports how a switchback attempt ended. An aborted revert
// leaves the backup path on air; a reverted event runs on primary again.
type RevertResult struct {
	EventID model.EventID   `json:"event_id"`
	Stage   RevertStage     `json:"stage"`
	Reason  string          `json:"reason,omitempty"`
	Path    audio.PathState `json:"path"`
}

// primaryHealthy confirms the recovered primary is locked and is delivering
// continuous pictures. It is re-evaluated at every switchback gate so a
// signal that drops mid-revert aborts the cut.
func (coordinator *Coordinator) primaryHealthy(eventID model.EventID, primarySource model.SourceID) (bool, string) {
	if !coordinator.sources.Ready(primarySource) {
		return false, "primary source is not locked"
	}
	status, ok := coordinator.sync.Status(eventID)
	if !ok {
		return false, "primary continuity window is not open"
	}
	if status.Source != primarySource {
		return false, "continuity window belongs to another source"
	}
	if !status.Stable {
		reason := status.Reason
		if reason == "" {
			reason = "primary pictures are not continuous"
		}
		return false, reason
	}
	return true, ""
}

func (coordinator *Coordinator) abortRevert(eventID model.EventID, stage RevertStage, reason string) RevertResult {
	// The audio layer retires any partially prepared primary media and
	// never touches the backup path, which stays playable throughout.
	_ = coordinator.audio.AbortSwitchback(eventID, string(stage)+":"+reason)
	coordinator.state.AbortRevert(eventID, coordinator.clock(), reason)
	path, _ := coordinator.audio.State().Path(eventID)
	return RevertResult{EventID: eventID, Stage: RevertAborted, Reason: reason, Path: path}
}

// Revert performs a safe switchback to the recovered primary:
// lock/continuity is confirmed before any media moves, primary video and
// then audio are prepared while backup keeps carrying the program, and the
// backup path (plus its encoder lease) is released only after primary is
// fully live. If the primary drops at any gate the attempt aborts and the
// current playable backup path is retained.
func (coordinator *Coordinator) Revert(event model.LiveEvent, primarySource model.SourceID) (RevertResult, error) {
	item, ok := coordinator.state.Get(event.ID)
	if !ok || item.Status != "active" {
		return RevertResult{EventID: event.ID, Stage: RevertAborted, Reason: "event is not failed over"},
			fmt.Errorf("event %s cannot switch back: backup path is not active", event.ID)
	}
	if item.Lease.Owner != event.ID {
		return RevertResult{EventID: event.ID, Stage: RevertAborted, Reason: "backup lease is missing"},
			fmt.Errorf("event %s cannot switch back without its backup lease", event.ID)
	}
	if !primarySource.Valid() {
		return RevertResult{EventID: event.ID, Stage: RevertAborted, Reason: "primary source is unknown"},
			fmt.Errorf("primary source for event %s is unknown", event.ID)
	}

	coordinator.state.BeginRevert(event.ID, coordinator.clock())
	if healthy, reason := coordinator.primaryHealthy(event.ID, primarySource); !healthy {
		result := coordinator.abortRevert(event.ID, RevertVerifyPrimary, reason)
		return result, fmt.Errorf("switchback refused: %s", reason)
	}

	coordinator.state.NoteRevert(event.ID, RevertPrepareVideo)
	if err := coordinator.audio.PreparePrimaryVideo(event.ID); err != nil {
		result := coordinator.abortRevert(event.ID, RevertPrepareVideo, err.Error())
		return result, err
	}
	if healthy, reason := coordinator.primaryHealthy(event.ID, primarySource); !healthy {
		result := coordinator.abortRevert(event.ID, RevertPrepareVideo, reason)
		return result, fmt.Errorf("switchback aborted: %s", reason)
	}

	coordinator.state.NoteRevert(event.ID, RevertPrepareAudio)
	if err := coordinator.audio.PreparePrimaryAudio(event.ID); err != nil {
		result := coordinator.abortRevert(event.ID, RevertPrepareAudio, err.Error())
		return result, err
	}
	if healthy, reason := coordinator.primaryHealthy(event.ID, primarySource); !healthy {
		result := coordinator.abortRevert(event.ID, RevertPrepareAudio, reason)
		return result, fmt.Errorf("switchback aborted: %s", reason)
	}

	coordinator.state.NoteRevert(event.ID, RevertReleaseBackup)
	if err := coordinator.audio.ReleaseBackup(event.ID); err != nil {
		result := coordinator.abortRevert(event.ID, RevertReleaseBackup, err.Error())
		return result, err
	}
	// Backup media is disconnected; freeing the encoder cannot interrupt
	// the program because primary now carries it.
	if err := coordinator.encoders.Release(item.Lease); err != nil {
		path, _ := coordinator.audio.State().Path(event.ID)
		return RevertResult{EventID: event.ID, Stage: RevertReverted, Reason: "backup encoder lease release failed", Path: path},
			fmt.Errorf("primary is live but backup lease release failed: %w", err)
	}
	coordinator.state.CompleteRevert(event.ID, coordinator.clock())
	path, _ := coordinator.audio.State().Path(event.ID)
	return RevertResult{EventID: event.ID, Stage: RevertReverted, Path: path}, nil
}

func (coordinator *Coordinator) ScheduleRevert(ctx context.Context, event model.LiveEvent, primarySource model.SourceID, delay time.Duration) <-chan error {
	return coordinator.playout.StartWorker(ctx, event, delay, func(workerContext context.Context) error {
		if err := workerContext.Err(); err != nil {
			return err
		}
		_, err := coordinator.Revert(event, primarySource)
		return err
	})
}
