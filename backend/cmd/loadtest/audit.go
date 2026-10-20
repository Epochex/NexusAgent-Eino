package main

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/gorm"
)

type distribution struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

func summarize(values []float64) distribution {
	if len(values) == 0 {
		return distribution{}
	}
	values = append([]float64(nil), values...)
	sort.Float64s(values)
	quantile := func(p float64) float64 { return values[max(0, int(math.Ceil(p*float64(len(values))))-1)] }
	return distribution{Count: len(values), P50: quantile(.5), P95: quantile(.95), P99: quantile(.99), Max: values[len(values)-1]}
}

type auditedWorkflow struct {
	ID         int64      `json:"id"`
	OwnerID    int64      `json:"owner_id"`
	RequestKey string     `json:"nonce"`
	Status     string     `json:"status"`
	Output     string     `json:"-"`
	CreatedAt  time.Time  `json:"-"`
	StartedAt  *time.Time `json:"-"`
	FinishedAt *time.Time `json:"-"`
}
type databaseAudit struct {
	WorkflowTotal           int               `json:"workflow_total"`
	WorkflowStates          map[string]int    `json:"workflow_states"`
	UnacknowledgedWorkflows int               `json:"unacknowledged_workflows"`
	InvalidWorkflowResults  int               `json:"invalid_workflow_results"`
	ChatTotal               int               `json:"chat_total"`
	ChatStates              map[string]int    `json:"chat_states"`
	DuplicateChatNonces     int               `json:"duplicate_chat_nonces"`
	InvalidChatResults      int               `json:"invalid_chat_results"`
	Queue                   distribution      `json:"workflow_queue"`
	Execution               distribution      `json:"workflow_execution"`
	WorkflowEndToEnd        distribution      `json:"workflow_end_to_end"`
	Workflows               []auditedWorkflow `json:"-"`
	CompletedNonces         []string          `json:"-"`
}

var auditPrefixPattern = regexp.MustCompile(`^load-[a-zA-Z0-9-]+-$`)

// Query the stage namespace in PG independently of HTTP responses. A lost
// response must not erase a committed request from acceptance accounting.
func auditDatabase(parent context.Context, db *gorm.DB, prefix string, acknowledged map[int64]bool) (*databaseAudit, error) {
	if !auditPrefixPattern.MatchString(prefix) {
		return nil, fmt.Errorf("invalid audit namespace")
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	report := &databaseAudit{WorkflowStates: map[string]int{}, ChatStates: map[string]int{}}
	if err := db.WithContext(ctx).Table("workflow_runs").Select("id,owner_id,request_key,status,output,created_at,started_at,finished_at").Where("definition IS NOT NULL AND request_key LIKE ?", prefix+"%").Order("id").Find(&report.Workflows).Error; err != nil {
		return nil, err
	}
	var queues, execution, totals []float64
	for _, row := range report.Workflows {
		report.WorkflowTotal++
		report.WorkflowStates[row.Status]++
		if !acknowledged[row.ID] {
			report.UnacknowledgedWorkflows++
		}
		if row.StartedAt != nil {
			queues = append(queues, float64(row.StartedAt.Sub(row.CreatedAt))/float64(time.Millisecond))
		}
		if row.FinishedAt != nil {
			totals = append(totals, float64(row.FinishedAt.Sub(row.CreatedAt))/float64(time.Millisecond))
			if row.StartedAt != nil {
				execution = append(execution, float64(row.FinishedAt.Sub(*row.StartedAt))/float64(time.Millisecond))
			}
		}
		if row.Status == "succeeded" {
			if !strings.Contains(row.Output, "completed nonce="+row.RequestKey) {
				report.InvalidWorkflowResults++
			}
			report.CompletedNonces = append(report.CompletedNonces, row.RequestKey)
		}
	}
	var chats []struct {
		SessionID      string
		RequestContent string
		Status         *int16
		Content        string
	}
	err := db.WithContext(ctx).Raw(`SELECT s.id AS session_id,u.content AS request_content,a.status,a.content
	 FROM messages u JOIN sessions s ON s.id=u.session_id
	 LEFT JOIN LATERAL (SELECT status,content FROM messages WHERE session_id=s.id AND role='assistant' ORDER BY id DESC LIMIT 1) a ON true
	 WHERE u.role='user' AND u.content LIKE ?`, "nonce="+prefix+"%").Scan(&chats).Error
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, row := range chats {
		report.ChatTotal++
		nonce := strings.TrimPrefix(row.RequestContent, "nonce=")
		if seen[nonce] {
			report.DuplicateChatNonces++
		}
		seen[nonce] = true
		status := "missing_terminal"
		if row.Status != nil {
			switch *row.Status {
			case model.MsgStatusDone:
				status = "done"
			case model.MsgStatusFailed:
				status = "failed"
			case model.MsgStatusStopped:
				status = "stopped"
			default:
				status = "unrecognized"
			}
		}
		report.ChatStates[status]++
		if status == "done" {
			if !strings.Contains(row.Content, "completed nonce="+nonce) {
				report.InvalidChatResults++
			}
			report.CompletedNonces = append(report.CompletedNonces, nonce)
		}
	}
	report.Queue = summarize(queues)
	report.Execution = summarize(execution)
	report.WorkflowEndToEnd = summarize(totals)
	return report, nil
}
