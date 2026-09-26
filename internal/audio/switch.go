package audio

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type Switch struct {
	state *State
	clock func() time.Time
}

func NewSwitch(state *State, clock func() time.Time) *Switch {
	return &Switch{state: state, clock: clock}
}

func (switcher *Switch) Prepare(eventID model.EventID) PathState {
	if path, ok := switcher.state.Path(eventID); ok {
		return path
	}
	return switcher.state.Open(eventID, switcher.clock())
}

func (switcher *Switch) Path(eventID model.EventID) (PathState, bool) {
	return switcher.state.Path(eventID)
}

func (switcher *Switch) RetirePrimary(eventID model.EventID) error {
	switcher.Prepare(eventID)
	if err := switcher.state.RetirePrimaryVideo(eventID, switcher.clock()); err != nil {
		return err
	}
	if err := switcher.state.MutePrimary(eventID, switcher.clock()); err != nil {
		return err
	}
	return nil
}

func (switcher *Switch) ConnectBackup(eventID model.EventID) error {
	if err := switcher.state.ConnectBackup(eventID, switcher.clock()); err != nil {
		return fmt.Errorf("backup audio switch refused: %w", err)
	}
	return nil
}

func (switcher *Switch) Failover(eventID model.EventID) error {
	if err := switcher.RetirePrimary(eventID); err != nil {
		return err
	}
	return switcher.ConnectBackup(eventID)
}

func (switcher *Switch) PreparePrimary(eventID model.EventID) error {
	if err := switcher.state.PreparePrimary(eventID, switcher.clock()); err != nil {
		return fmt.Errorf("primary audio prepare refused: %w", err)
	}
	return nil
}

func (switcher *Switch) ReleaseBackup(eventID model.EventID) error {
	if err := switcher.state.ReleaseBackup(eventID, switcher.clock()); err != nil {
		return fmt.Errorf("backup audio release refused: %w", err)
	}
	return nil
}

func (switcher *Switch) Failback(eventID model.EventID) error {
	if err := switcher.PreparePrimary(eventID); err != nil {
		return err
	}
	return switcher.ReleaseBackup(eventID)
}
