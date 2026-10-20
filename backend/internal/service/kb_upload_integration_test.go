//go:build integration

package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/pkg/errcode"
)

func TestUploadWithoutObjectStoreCannotLoseSource(t *testing.T) {
	_, services, owner := workflowServiceFixture(t)
	db := services.Repos.DB
	provider := &model.ModelProvider{Code: "embed", Name: "embed", BaseURL: "http://127.0.0.1:1", Status: 1}
	if err := db.Create(provider).Error; err != nil {
		t.Fatal(err)
	}
	configured := &model.ModelConfig{ProviderID: provider.ID, Alias: "embed", ModelName: "embed", Type: model.ModelTypeEmbed, Status: 1}
	if err := db.Create(configured).Error; err != nil {
		t.Fatal(err)
	}
	kb := &model.KnowledgeBase{Name: "source", UserID: owner, EmbeddingConfigID: configured.ID, EmbeddingVersion: strings.Repeat("a", 64), EmbeddingDimension: 3, MilvusCollection: "source"}
	if err := db.Create(kb).Error; err != nil {
		t.Fatal(err)
	}
	service := NewKBService(services)
	_, err := service.UploadDoc(context.Background(), owner, kb.ID, "source.txt", []byte("must not disappear"))
	var be *errcode.Error
	if !errors.As(err, &be) || be.HTTPStatus != 503 {
		t.Fatalf("missing object storage accepted source: %v", err)
	}
	var count int64
	db.Model(&model.Document{}).Where("kb_id=?", kb.ID).Count(&count)
	if count != 0 {
		t.Fatalf("unrecoverable document created: %d", count)
	}
	_, err = service.UploadDoc(context.Background(), owner+1, kb.ID, "source.txt", []byte("foreign"))
	if !errors.As(err, &be) || be.HTTPStatus != 404 {
		t.Fatalf("foreign upload leaked/used KB: %v", err)
	}
}
