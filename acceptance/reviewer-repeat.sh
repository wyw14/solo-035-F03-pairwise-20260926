#!/bin/sh
set -eu
cd /workspace
/opt/go/bin/go version
/opt/go/bin/go test ./internal/service -run '^TestSwitchbackFlowEndToEnd$' -count=50 -v
