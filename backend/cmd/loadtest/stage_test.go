package main

import "testing"

func TestCapacityTargetDoesNotPassByRejectingOfferedWork(t *testing.T) {
	r := &stageReport{Wire: 2, Arrivals: arrivalSnapshot{Target: 2}, Outcomes: map[string]int{"chat_rejected": 1}, Database: &databaseAudit{WorkflowTotal: 1, WorkflowStates: map[string]int{"succeeded": 1}}, Latency: map[string]distribution{"control_query": {Count: 1}, "control_approve": {Count: 1}, "control_cancel": {Count: 1}}}
	assessStage(r)
	if r.Passed {
		t.Fatal("offered capacity target incorrectly passed by rejecting half the work")
	}
}

func TestCapacityTargetDoesNotHideOffersAfterWindow(t *testing.T) {
	r := &stageReport{Wire: 2, WireAfterWindow: 1, Arrivals: arrivalSnapshot{Target: 2}, Outcomes: map[string]int{}, Database: &databaseAudit{WorkflowTotal: 2, WorkflowStates: map[string]int{"succeeded": 2}}, Latency: map[string]distribution{"control_query": {Count: 1}, "control_approve": {Count: 1}, "control_cancel": {Count: 1}}}
	assessStage(r)
	if r.Passed {
		t.Fatal("late HTTP offers hidden behind scheduled callback counts")
	}
}

func TestCapacityTargetDoesNotHideClientTerminalLossBehindDBSuccess(t *testing.T) {
	r := &stageReport{Wire: 1, Arrivals: arrivalSnapshot{Target: 1}, Outcomes: map[string]int{"chat_missing_terminal": 1}, Database: &databaseAudit{ChatTotal: 1, WorkflowStates: map[string]int{}, ChatStates: map[string]int{"done": 1}}, Latency: map[string]distribution{"control_query": {Count: 1}, "control_approve": {Count: 1}, "control_cancel": {Count: 1}}}
	assessStage(r)
	if r.Passed {
		t.Fatal("missing client terminal hidden behind persisted success")
	}
}
