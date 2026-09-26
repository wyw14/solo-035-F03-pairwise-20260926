package service

import (
	"fmt"

	"github.com/wyw14/solo-035/internal/journal"
	"github.com/wyw14/solo-035/internal/model"
)

type RecoveryReport struct {
	LoadedPrograms int      `json:"loaded_programs"`
	StartedOutputs int      `json:"started_outputs"`
	Trace          []string `json:"trace"`
}

func (runtime *Runtime) Recover(sourceFor func(model.EventID) model.SourceID) (RecoveryReport, error) {
	records, err := runtime.Journal.Recover()
	if err != nil {
		return RecoveryReport{}, err
	}
	results, err := journal.Replay(records)
	if err != nil {
		return RecoveryReport{}, err
	}
	report := RecoveryReport{}
	valid := make([]journal.ReplayResult, 0, len(results))
	for _, result := range results {
		if result.Revision == "" || result.Fence == 0 {
			continue
		}
		valid = append(valid, result)
		report.LoadedPrograms++
		report.Trace = append(report.Trace, "loaded-program-generation")
	}
	if err := runtime.PlayoutRecovery.RestoreAll(valid, sourceFor); err != nil {
		return report, err
	}
	for range valid {
		report.StartedOutputs++
		report.Trace = append(report.Trace, "started-playout-worker")
	}
	if report.LoadedPrograms != report.StartedOutputs {
		return report, fmt.Errorf("recovery did not start every restored program")
	}
	return report, nil
}

func (runtime *Runtime) RestoreFromRecords(records []journal.Record, sourceFor func(model.EventID) model.SourceID) (RecoveryReport, error) {
	runtime.JournalState.Reset()
	for _, record := range records {
		runtime.JournalState.Apply(record)
	}
	results, err := journal.Replay(runtime.JournalState.Records())
	if err != nil {
		return RecoveryReport{}, err
	}
	report := RecoveryReport{}
	for _, result := range results {
		if result.Revision == "" || result.Fence == 0 {
			continue
		}
		report.LoadedPrograms++
		report.Trace = append(report.Trace, "loaded-program-generation")
		if err := runtime.PlayoutRecovery.Restore(result, sourceFor(result.EventID), "recovered-current-segment"); err != nil {
			return report, err
		}
		report.StartedOutputs++
		report.Trace = append(report.Trace, "started-playout-worker")
	}
	return report, nil
}
