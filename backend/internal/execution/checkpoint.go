package execution

import (
	"context"
	"errors"
	"github.com/cloudwego/eino/compose"
	"gorm.io/gorm"
	"time"
)

type CheckpointStore struct {
	store *Store
	run   Run
}

var _ compose.CheckPointStore = (*CheckpointStore)(nil)

func (s *Store) Checkpoints(run *Run) *CheckpointStore { return &CheckpointStore{store: s, run: *run} }
func (c *CheckpointStore) Get(ctx context.Context, id string) ([]byte, bool, error) {
	if len(id) > 512 {
		return nil, false, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var row struct {
		Payload        []byte
		DefinitionHash string
		EngineVersion  string
		GraphSchema    int
	}
	err := c.store.db.WithContext(ctx).Table("execution_checkpoints").Where("run_id=? AND checkpoint_id=?", c.run.ID, id).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if row.DefinitionHash != c.run.DefinitionHash || row.EngineVersion != c.run.EngineVersion || row.GraphSchema != c.run.GraphSchema || c.run.EngineVersion != EngineVersion || c.run.GraphSchema != GraphSchema {
		return nil, false, ErrVersionMismatch
	}
	return row.Payload, true, nil
}
func (c *CheckpointStore) Set(ctx context.Context, id string, payload []byte) error {
	if len(id) > 512 || len(payload) > 16<<20 {
		return ErrInvalidInput
	}
	return c.store.transaction(ctx, func(tx *gorm.DB) error {
		if err := lockLease(tx, &c.run); err != nil {
			return err
		}
		result := tx.Exec(`INSERT INTO execution_checkpoints(run_id,checkpoint_id,definition_hash,engine_version,graph_schema,payload) VALUES (?,?,?,?,?,?) ON CONFLICT (run_id,checkpoint_id) DO UPDATE SET payload=EXCLUDED.payload,updated_at=NOW() WHERE execution_checkpoints.definition_hash=EXCLUDED.definition_hash AND execution_checkpoints.engine_version=EXCLUDED.engine_version AND execution_checkpoints.graph_schema=EXCLUDED.graph_schema`, c.run.ID, id, c.run.DefinitionHash, c.run.EngineVersion, c.run.GraphSchema, payload)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrVersionMismatch
		}
		return nil
	})
}
