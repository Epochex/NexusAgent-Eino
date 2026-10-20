package execution

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type definitionRelease struct {
	ID             int64
	WorkflowID     int64
	Version        int
	DefinitionHash string
	Definition     datatypes.JSON
	CreatedAt      time.Time
}

func (definitionRelease) TableName() string { return "workflow_versions" }
func publishSnapshot(tx *gorm.DB, run *Run, version int) error {
	var metadata struct {
		Version int `json:"workflow_version"`
	}
	if err := json.Unmarshal(run.Definition, &metadata); err != nil {
		return err
	}
	if metadata.Version > 0 && metadata.Version != version {
		return ErrConflict
	}
	release := definitionRelease{WorkflowID: run.WorkflowID, Version: version, DefinitionHash: run.DefinitionHash, Definition: run.Definition}
	result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "workflow_id"}, {Name: "definition_hash"}}, DoNothing: true}).Create(&release)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		if err := tx.Where("workflow_id=? AND definition_hash=?", run.WorkflowID, run.DefinitionHash).Take(&release).Error; err != nil {
			return err
		}
	}
	run.DefinitionReleaseID = &release.ID
	return nil
}
func findRequest(tx *gorm.DB, ownerID, workflowID int64, key string, input []byte) (*Run, error) {
	var run Run
	err := tx.Where("owner_id=? AND request_key=?", ownerID, key).Take(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	normalized, err := object(run.Input, 1<<20)
	if err != nil {
		return nil, err
	}
	if run.WorkflowID != workflowID || string(normalized) != string(input) {
		return nil, ErrConflict
	}
	return &run, nil
}
func (s *Store) FindRequest(ctx context.Context, ownerID, workflowID int64, key string, input json.RawMessage) (*Run, error) {
	if key == "" {
		return nil, nil
	}
	if len(key) > 64 {
		return nil, ErrInvalidInput
	}
	normalized, err := object(input, 1<<20)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return findRequest(s.db.WithContext(ctx), ownerID, workflowID, key, normalized)
}
