package audio

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

// PathUsage records one media path that actually carried the program,
// including why it was entered and when it was released.
type PathUsage struct {
	Route     string     `json:"route"` // "primary" or "backup"
	Reason    string     `json:"reason"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

type PathState struct {
	EventID          model.EventID `json:"event_id"`
	PrimaryVideoLive bool          `json:"primary_video_live"`
	PrimaryAudioLive bool          `json:"primary_audio_live"`
	BackupVideoLive  bool          `json:"backup_video_live"`
	BackupAudioLive  bool          `json:"backup_audio_live"`
	UpdatedAt        time.Time     `json:"updated_at"`
	Sequence         []string      `json:"sequence"`
	Usage            []PathUsage   `json:"usage"`
}

type State struct {
	mu    sync.RWMutex
	paths map[model.EventID]PathState
}

func NewState() *State {
	return &State{paths: make(map[model.EventID]PathState)}
}

func (state *State) clone(path PathState) PathState {
	path.Sequence = append([]string(nil), path.Sequence...)
	path.Usage = append([]PathUsage(nil), path.Usage...)
	return path
}

func (state *State) Open(eventID model.EventID, now time.Time) PathState {
	state.mu.Lock()
	defer state.mu.Unlock()
	path := PathState{
		EventID:          eventID,
		PrimaryVideoLive: true,
		PrimaryAudioLive: true,
		UpdatedAt:        now.UTC(),
	}
	path.Sequence = append(path.Sequence, "primary-open")
	path.Usage = append(path.Usage, PathUsage{Route: "primary", Reason: "primary-open", StartedAt: now.UTC()})
	state.paths[eventID] = path
	return state.clone(path)
}

func (state *State) RetirePrimaryVideo(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	path.PrimaryVideoLive = false
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "retire-primary-video")
	state.paths[eventID] = path
	return nil
}

func (state *State) MutePrimary(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	path.PrimaryAudioLive = false
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "mute-primary-audio")
	state.closeUsage(&path, "primary", now, "primary-muted")
	state.paths[eventID] = path
	return nil
}

func (state *State) ConnectBackup(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	if path.PrimaryVideoLive || path.PrimaryAudioLive {
		return fmt.Errorf("primary media must retire before backup connects")
	}
	path.BackupVideoLive = true
	path.BackupAudioLive = true
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "connect-backup-media")
	path.Usage = append(path.Usage, PathUsage{Route: "backup", Reason: "failover", StartedAt: now.UTC()})
	state.paths[eventID] = path
	return nil
}

// PreparePrimaryVideo brings primary video back while the backup path keeps
// carrying the program, so the air path never goes dark during switchback.
func (state *State) PreparePrimaryVideo(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	if !path.BackupVideoLive || !path.BackupAudioLive {
		return fmt.Errorf("primary video can only be prepared while backup is on air")
	}
	if path.PrimaryVideoLive {
		return fmt.Errorf("primary video is already prepared")
	}
	path.PrimaryVideoLive = true
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "prepare-primary-video")
	state.paths[eventID] = path
	return nil
}

// PreparePrimaryAudio unmutes primary audio after primary video is confirmed.
// Backup audio remains live until this step completes, so the program keeps a
// playable audio path right up to the cut.
func (state *State) PreparePrimaryAudio(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	if !path.BackupVideoLive || !path.BackupAudioLive {
		return fmt.Errorf("primary audio can only be prepared while backup is on air")
	}
	if !path.PrimaryVideoLive {
		return fmt.Errorf("primary video must be confirmed before primary audio")
	}
	if path.PrimaryAudioLive {
		return fmt.Errorf("primary audio is already prepared")
	}
	path.PrimaryAudioLive = true
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "prepare-primary-audio")
	path.Usage = append(path.Usage, PathUsage{Route: "primary", Reason: "switchback", StartedAt: now.UTC()})
	state.paths[eventID] = path
	return nil
}

// DisconnectBackup releases the backup path. It is only accepted once the
// primary path fully carries video and audio.
func (state *State) DisconnectBackup(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive {
		return fmt.Errorf("backup cannot release before primary media is live")
	}
	if !path.BackupVideoLive && !path.BackupAudioLive {
		return fmt.Errorf("backup media is already disconnected")
	}
	path.BackupVideoLive = false
	path.BackupAudioLive = false
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "disconnect-backup-media")
	state.closeUsage(&path, "backup", now, "backup-released")
	state.paths[eventID] = path
	return nil
}

// AbortSwitchback discards a partially prepared primary path and returns the
// switch to the backup-only on-air shape. The backup path is never touched,
// so the current playable path is preserved.
func (state *State) AbortSwitchback(eventID model.EventID, now time.Time, reason string) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	if !path.BackupVideoLive || !path.BackupAudioLive {
		return fmt.Errorf("switchback can only abort while backup is on air")
	}
	changed := false
	if path.PrimaryVideoLive {
		path.PrimaryVideoLive = false
		changed = true
	}
	if path.PrimaryAudioLive {
		path.PrimaryAudioLive = false
		changed = true
		state.closeUsage(&path, "primary", now, "switchback-aborted")
	}
	if !changed {
		return nil
	}
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "abort-switchback:"+reason)
	state.paths[eventID] = path
	return nil
}

func (state *State) closeUsage(path *PathState, route string, now time.Time, reason string) {
	for index := len(path.Usage) - 1; index >= 0; index-- {
		usage := &path.Usage[index]
		if usage.Route == route && usage.EndedAt == nil {
			ended := now.UTC()
			usage.EndedAt = &ended
			if reason != "" {
				path.Sequence = append(path.Sequence, route+"-usage-end:"+reason)
			}
			return
		}
	}
}

func (state *State) Path(eventID model.EventID) (PathState, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	path, ok := state.paths[eventID]
	return state.clone(path), ok
}

func (state *State) List() []PathState {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]PathState, 0, len(state.paths))
	for _, path := range state.paths {
		items = append(items, state.clone(path))
	}
	return items
}
