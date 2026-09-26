package backup

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type RecoveryRecord struct {
	EventID      model.EventID      `json:"event_id"`
	Command      model.CommandID    `json:"command"`
	Lease        model.EncoderLease `json:"lease"`
	Status       string             `json:"status"`
	RevertStage  string             `json:"revert_stage,omitempty"`
	RevertReason string             `json:"revert_reason,omitempty"`
	RevertedAt   *time.Time         `json:"reverted_at,omitempty"`
}

func (state *State) ExportRecovery() []RecoveryRecord {
	items := state.List()
	records := make([]RecoveryRecord, 0, len(items))
	for _, item := range items {
		records = append(records, RecoveryRecord{
			EventID:      item.EventID,
			Command:      item.Command,
			Lease:        item.Lease,
			Status:       item.Status,
			RevertStage:  item.RevertStage,
			RevertReason: item.RevertReason,
			RevertedAt:   item.RevertedAt,
		})
	}
	return records
}

func (state *State) RestoreRecovery(records []RecoveryRecord) error {
	for _, record := range records {
		if !record.EventID.Valid() || !record.Command.Valid() {
			return fmt.Errorf("backup recovery identity is invalid")
		}
		state.mu.Lock()
		state.failovers[record.EventID] = FailoverState{
			EventID:      record.EventID,
			Command:      record.Command,
			Lease:        record.Lease,
			Status:       record.Status,
			RevertStage:  record.RevertStage,
			RevertReason: record.RevertReason,
			RevertedAt:   record.RevertedAt,
		}
		state.mu.Unlock()
	}
	return nil
}

func (state *State) GetOrZero(eventID model.EventID) FailoverState {
	item, _ := state.Get(eventID)
	return item
}
