package service

import (
	"context"
	"testing"
	"time"
)

func bootstrappedRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(t.TempDir(), func() time.Time { return time.Unix(100, 0).UTC() })
	if err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(runtime); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func mediaPathRecords(t *testing.T, runtime *Runtime) []map[string]any {
	t.Helper()
	var payloads []map[string]any
	for _, record := range runtime.Journal.Records() {
		if record.Kind != "media-path" {
			continue
		}
		var payload map[string]any
		if err := record.Decode(&payload); err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, payload)
	}
	return payloads
}

func TestFailbackRestoresPrimaryAndRecordsMediaPath(t *testing.T) {
	runtime := bootstrappedRuntime(t)
	event := runtime.Events.List()[0]
	sourceID := runtime.SourceFlow.Snapshot()[0].Source
	ctx := context.Background()

	if _, err := runtime.ExecuteControl(ctx, ControlRequest{Action: "audio-failover", EventID: string(event.ID)}); err != nil {
		t.Fatal(err)
	}
	path, _ := runtime.AudioState.Path(event.ID)
	if path.Active() != "backup" {
		t.Fatalf("active = %s after failover", path.Active())
	}

	result, err := runtime.ExecuteControl(ctx, ControlRequest{Action: "audio-failback", EventID: string(event.ID), SourceID: string(sourceID)})
	if err != nil {
		t.Fatal(err)
	}
	failback, ok := result.Value.(FailbackResult)
	if !ok {
		t.Fatalf("value type = %T", result.Value)
	}
	if !failback.Restored || failback.ActivePath != "primary" {
		t.Fatalf("restored = %v active = %s", failback.Restored, failback.ActivePath)
	}
	if failback.Failover != nil {
		t.Fatalf("failover = %+v, want no record for audio-only failover", failback.Failover)
	}
	path, _ = runtime.AudioState.Path(event.ID)
	if path.Active() != "primary" || path.BackupLive() {
		t.Fatalf("path = %+v, want primary only", path)
	}

	records := mediaPathRecords(t, runtime)
	if len(records) != 2 {
		t.Fatalf("media-path records = %d, want failover and failback entries", len(records))
	}
	if records[0]["active_path"] != "backup" || records[0]["trigger"] != "audio-failover" {
		t.Fatalf("first record = %v", records[0])
	}
	if records[1]["active_path"] != "primary" || records[1]["restored"] != true {
		t.Fatalf("second record = %v", records[1])
	}

	checkpoint := runtime.ExportCheckpoint()
	if _, err := runtime.RestoreCheckpoint(checkpoint); err != nil {
		t.Fatalf("media-path audit records must not disturb recovery: %v", err)
	}
}

func TestFailbackRejectedWhenPrimaryNotReady(t *testing.T) {
	runtime := bootstrappedRuntime(t)
	event := runtime.Events.List()[0]
	sourceID := runtime.SourceFlow.Snapshot()[0].Source
	ctx := context.Background()

	if _, err := runtime.ExecuteControl(ctx, ControlRequest{Action: "audio-failover", EventID: string(event.ID)}); err != nil {
		t.Fatal(err)
	}
	coordinator := NewCoordinator(runtime)
	if _, err := coordinator.PrepareSignal(event.ID, sourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ExecuteControl(ctx, ControlRequest{Action: "audio-failback", EventID: string(event.ID), SourceID: string(sourceID)}); err == nil {
		t.Fatal("expected failback to be rejected while primary is not locked and continuous")
	}
	path, _ := runtime.AudioState.Path(event.ID)
	if path.Active() != "backup" {
		t.Fatalf("active = %s, want backup retained", path.Active())
	}
	if records := mediaPathRecords(t, runtime); len(records) != 1 {
		t.Fatalf("media-path records = %d, want only the failover entry", len(records))
	}
}

func TestScheduleFailoverRecordsMediaPath(t *testing.T) {
	runtime := bootstrappedRuntime(t)
	event := runtime.Events.List()[0]
	if _, err := runtime.ExecuteControl(context.Background(), ControlRequest{Action: "schedule-failover", EventID: string(event.ID)}); err != nil {
		t.Fatal(err)
	}
	records := mediaPathRecords(t, runtime)
	if len(records) != 1 || records[0]["active_path"] != "backup" || records[0]["trigger"] != "schedule-failover" {
		t.Fatalf("records = %v", records)
	}
}

func TestScheduledFailoverThenFailbackRestoresLease(t *testing.T) {
	runtime := bootstrappedRuntime(t)
	event := runtime.Events.List()[0]
	sourceID := runtime.SourceFlow.Snapshot()[0].Source
	ctx := context.Background()

	if _, err := runtime.ExecuteControl(ctx, ControlRequest{Action: "schedule-failover", EventID: string(event.ID)}); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ExecuteControl(ctx, ControlRequest{Action: "audio-failback", EventID: string(event.ID), SourceID: string(sourceID)})
	if err != nil {
		t.Fatal(err)
	}
	failback := result.Value.(FailbackResult)
	if failback.Failover == nil || failback.Failover.Status != "restored" || failback.Failover.RestoredAt == nil {
		t.Fatalf("failover = %+v, want restored record", failback.Failover)
	}
	if owner := runtime.Encoders.Owner(failback.Failover.Lease.EncoderID); owner != "" {
		t.Fatalf("backup encoder lease still owned by %s", owner)
	}
	records := mediaPathRecords(t, runtime)
	if len(records) != 2 || records[1]["failover_status"] != "restored" {
		t.Fatalf("records = %v", records)
	}
}
