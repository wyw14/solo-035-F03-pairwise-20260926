package audio

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type PathState struct {
	EventID          model.EventID `json:"event_id"`
	PrimaryVideoLive bool          `json:"primary_video_live"`
	PrimaryAudioLive bool          `json:"primary_audio_live"`
	BackupVideoLive  bool          `json:"backup_video_live"`
	BackupAudioLive  bool          `json:"backup_audio_live"`
	UpdatedAt        time.Time     `json:"updated_at"`
	Sequence         []string      `json:"sequence"`
}

type State struct {
	mu    sync.RWMutex
	paths map[model.EventID]PathState
}

func NewState() *State {
	return &State{paths: make(map[model.EventID]PathState)}
}

func (state *State) Open(eventID model.EventID, now time.Time) PathState {
	state.mu.Lock()
	defer state.mu.Unlock()
	path := PathState{EventID: eventID, PrimaryVideoLive: true, PrimaryAudioLive: true, UpdatedAt: now.UTC()}
	path.Sequence = append(path.Sequence, "primary-open")
	state.paths[eventID] = path
	return path
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
	state.paths[eventID] = path
	return nil
}

func (state *State) PreparePrimary(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	if !path.BackupVideoLive || !path.BackupAudioLive {
		return fmt.Errorf("backup media must carry the program before primary is prepared")
	}
	if path.PrimaryVideoLive || path.PrimaryAudioLive {
		return fmt.Errorf("primary media is already prepared")
	}
	path.PrimaryVideoLive = true
	path.PrimaryAudioLive = true
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "prepare-primary-media")
	state.paths[eventID] = path
	return nil
}

func (state *State) ReleaseBackup(eventID model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	path, ok := state.paths[eventID]
	if !ok {
		return fmt.Errorf("audio path for event %s does not exist", eventID)
	}
	if !path.PrimaryVideoLive || !path.PrimaryAudioLive {
		return fmt.Errorf("primary media must be live before backup releases")
	}
	path.BackupVideoLive = false
	path.BackupAudioLive = false
	path.UpdatedAt = now.UTC()
	path.Sequence = append(path.Sequence, "release-backup-media")
	state.paths[eventID] = path
	return nil
}

func (path PathState) PrimaryLive() bool {
	return path.PrimaryVideoLive && path.PrimaryAudioLive
}

func (path PathState) BackupLive() bool {
	return path.BackupVideoLive && path.BackupAudioLive
}

func (path PathState) Active() string {
	switch {
	case path.PrimaryLive() && path.BackupLive():
		return "both"
	case path.PrimaryLive():
		return "primary"
	case path.BackupLive():
		return "backup"
	default:
		return "none"
	}
}

func (state *State) Path(eventID model.EventID) (PathState, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	path, ok := state.paths[eventID]
	path.Sequence = append([]string(nil), path.Sequence...)
	return path, ok
}

func (state *State) List() []PathState {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]PathState, 0, len(state.paths))
	for _, path := range state.paths {
		path.Sequence = append([]string(nil), path.Sequence...)
		items = append(items, path)
	}
	return items
}
