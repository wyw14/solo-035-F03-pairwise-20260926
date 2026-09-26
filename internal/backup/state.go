package backup

import (
	"sync"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type FailoverState struct {
	EventID      model.EventID      `json:"event_id"`
	Command      model.CommandID    `json:"command"`
	Lease        model.EncoderLease `json:"lease"`
	Status       string             `json:"status"`
	StartedAt    time.Time          `json:"started_at"`
	CompletedAt  *time.Time         `json:"completed_at,omitempty"`
	RevertStage  string             `json:"revert_stage,omitempty"`
	RevertReason string             `json:"revert_reason,omitempty"`
	RevertedAt   *time.Time         `json:"reverted_at,omitempty"`
}

type State struct {
	mu        sync.RWMutex
	failovers map[model.EventID]FailoverState
}

func NewState() *State {
	return &State{failovers: make(map[model.EventID]FailoverState)}
}

func (state *State) Begin(eventID model.EventID, now time.Time) FailoverState {
	state.mu.Lock()
	defer state.mu.Unlock()
	item := FailoverState{EventID: eventID, Command: model.NewCommandID(), Status: "preparing", StartedAt: now.UTC()}
	state.failovers[eventID] = item
	return item
}

func (state *State) AttachLease(eventID model.EventID, lease model.EncoderLease) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	item, ok := state.failovers[eventID]
	if !ok || lease.Owner != eventID {
		return false
	}
	item.Lease = lease
	item.Status = "reserved"
	state.failovers[eventID] = item
	return true
}

func (state *State) Complete(eventID model.EventID, now time.Time) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	item, ok := state.failovers[eventID]
	if !ok || item.Lease.Owner != eventID {
		return false
	}
	completed := now.UTC()
	item.Status = "active"
	item.CompletedAt = &completed
	state.failovers[eventID] = item
	return true
}

func (state *State) Cancel(eventID model.EventID) {
	state.mu.Lock()
	defer state.mu.Unlock()
	item := state.failovers[eventID]
	item.Status = "cancelled"
	state.failovers[eventID] = item
}

func (state *State) BeginRevert(eventID model.EventID, now time.Time) {
	state.mu.Lock()
	defer state.mu.Unlock()
	item, ok := state.failovers[eventID]
	if !ok {
		return
	}
	item.RevertStage = "verify-primary"
	item.RevertReason = ""
	item.RevertedAt = nil
	state.failovers[eventID] = item
}

func (state *State) NoteRevert(eventID model.EventID, stage RevertStage) {
	state.mu.Lock()
	defer state.mu.Unlock()
	item, ok := state.failovers[eventID]
	if !ok {
		return
	}
	item.RevertStage = string(stage)
	item.RevertReason = ""
	state.failovers[eventID] = item
}

func (state *State) AbortRevert(eventID model.EventID, now time.Time, reason string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	item, ok := state.failovers[eventID]
	if !ok {
		return
	}
	item.RevertStage = "aborted"
	item.RevertReason = reason
	// Backup remains the active on-air path after an aborted switchback.
	item.Status = "active"
	state.failovers[eventID] = item
}

func (state *State) CompleteRevert(eventID model.EventID, now time.Time) {
	state.mu.Lock()
	defer state.mu.Unlock()
	item, ok := state.failovers[eventID]
	if !ok {
		return
	}
	completed := now.UTC()
	item.Status = "reverted"
	item.RevertStage = "reverted"
	item.RevertReason = ""
	item.RevertedAt = &completed
	item.Lease = model.EncoderLease{}
	state.failovers[eventID] = item
}

func (state *State) Get(eventID model.EventID) (FailoverState, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	item, ok := state.failovers[eventID]
	return item, ok
}

func (state *State) List() []FailoverState {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]FailoverState, 0, len(state.failovers))
	for _, item := range state.failovers {
		items = append(items, item)
	}
	return items
}
