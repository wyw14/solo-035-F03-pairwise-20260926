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

func (switcher *Switch) State() *State {
	return switcher.state
}

func (switcher *Switch) Prepare(eventID model.EventID) PathState {
	if path, ok := switcher.state.Path(eventID); ok {
		return path
	}
	return switcher.state.Open(eventID, switcher.clock())
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

// PreparePrimary brings the primary media back while backup keeps carrying
// the program. Video is prepared first and audio only follows after the
// caller has re-confirmed primary lock and continuous pictures.
func (switcher *Switch) PreparePrimaryVideo(eventID model.EventID) error {
	if err := switcher.state.PreparePrimaryVideo(eventID, switcher.clock()); err != nil {
		return fmt.Errorf("primary video preparation refused: %w", err)
	}
	return nil
}

func (switcher *Switch) PreparePrimaryAudio(eventID model.EventID) error {
	if err := switcher.state.PreparePrimaryAudio(eventID, switcher.clock()); err != nil {
		return fmt.Errorf("primary audio preparation refused: %w", err)
	}
	return nil
}

// ReleaseBackup disconnects the backup path once primary media is fully live.
func (switcher *Switch) ReleaseBackup(eventID model.EventID) error {
	if err := switcher.state.DisconnectBackup(eventID, switcher.clock()); err != nil {
		return fmt.Errorf("backup release refused: %w", err)
	}
	return nil
}

// AbortSwitchback keeps the current playable (backup) path and retires any
// partially prepared primary media.
func (switcher *Switch) AbortSwitchback(eventID model.EventID, reason string) error {
	if err := switcher.state.AbortSwitchback(eventID, switcher.clock(), reason); err != nil {
		return fmt.Errorf("switchback abort refused: %w", err)
	}
	return nil
}
