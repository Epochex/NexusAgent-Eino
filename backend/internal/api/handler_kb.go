// 知识库接口：KB CRUD / 文档上传 / 分块查看 / 检索测试台。
package api

import (
	"context"
	"io"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/service"
)

// ListKBs GET /api/v1/kbs
func (h *Handler) ListKBs(ctx context.Context, c *app.RequestContext) {
	list, total, err := h.svcs.KB.ListKBs(ctx, userID(c), parsePagination(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": list, "total": total})
}

// GetKB GET /api/v1/kbs/:id
func (h *Handler) GetKB(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	kb, err := h.svcs.KB.GetKB(ctx, userID(c), id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, kb)
}

// CreateKB POST /api/v1/kbs
func (h *Handler) CreateKB(ctx context.Context, c *app.RequestContext) {
	kb := &model.KnowledgeBase{}
	if err := c.BindAndValidate(kb); err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.KB.CreateKB(ctx, userID(c), kb); err != nil {
		Fail(c, err)
		return
	}
	OK(c, kb)
}

// DeleteKB DELETE /api/v1/kbs/:id
func (h *Handler) DeleteKB(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.KB.DeleteKB(ctx, userID(c), id); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

// UploadDoc POST /api/v1/kbs/:id/documents（multipart 文件上传）。
func (h *Handler) UploadDoc(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	fh, err := c.FormFile("file")
	if err != nil {
		Fail(c, err)
		return
	}
	f, err := fh.Open()
	if err != nil {
		Fail(c, err)
		return
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, service.MaxDocSize+1))
	if err != nil {
		Fail(c, err)
		return
	}
	doc, err := h.svcs.KB.UploadDoc(ctx, userID(c), id, fh.Filename, data)
	if err != nil {
		Fail(c, err)
		return
	}
	c.JSON(202, envelope{Code: 0, Message: "accepted", Data: doc})
}

// ListDocs GET /api/v1/kbs/:id/documents
func (h *Handler) ListDocs(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	list, total, err := h.svcs.KB.ListDocs(ctx, userID(c), id, parsePagination(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": list, "total": total})
}

// DeleteDoc DELETE /api/v1/kbs/:id/documents/:docID
func (h *Handler) DeleteDoc(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	docID, err := pathIDNamed(c, "docID")
	if err != nil {
		Fail(c, err)
		return
	}
	if err := h.svcs.KB.DeleteDoc(ctx, userID(c), id, docID); err != nil {
		Fail(c, err)
		return
	}
	OK(c, nil)
}

// ListChunks GET /api/v1/documents/:docID/chunks —— 分块查看（ETL 质量排查）。
func (h *Handler) ListChunks(ctx context.Context, c *app.RequestContext) {
	docID, err := pathIDNamed(c, "docID")
	if err != nil {
		Fail(c, err)
		return
	}
	list, total, err := h.svcs.KB.ListChunks(ctx, userID(c), docID, parsePagination(c))
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, map[string]any{"list": list, "total": total})
}

// TestRetrieve POST /api/v1/kbs/:id/retrieve —— 检索测试台（调试召回质量）。
func (h *Handler) TestRetrieve(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	var req struct {
		Query string `json:"query"`
	}
	if err := c.BindAndValidate(&req); err != nil {
		Fail(c, err)
		return
	}
	refs, err := h.svcs.KB.TestRetrieve(ctx, userID(c), id, req.Query)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, refs)
}

func (h *Handler) RetrievalDetails(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	var request struct {
		Query string `json:"query"`
	}
	if err = c.BindAndValidate(&request); err != nil {
		Fail(c, err)
		return
	}
	result, err := h.svcs.KB.InspectRetrieval(ctx, userID(c), id, request.Query)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, result)
}

// pathIDNamed 按名字解析路径参数。
func (h *Handler) InitializeKB(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	kb, err := h.svcs.KB.InitializeKB(ctx, userID(c), id)
	if err != nil {
		Fail(c, err)
		return
	}
	OK(c, kb)
}
func (h *Handler) RetryDoc(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	docID, err := pathIDNamed(c, "docID")
	if err != nil {
		Fail(c, err)
		return
	}
	if err = h.svcs.KB.RetryDoc(ctx, userID(c), id, docID); err != nil {
		Fail(c, err)
		return
	}
	c.JSON(202, envelope{Code: 0, Message: "accepted"})
}
func (h *Handler) ReindexDoc(ctx context.Context, c *app.RequestContext) {
	id, err := pathID(c)
	if err != nil {
		Fail(c, err)
		return
	}
	docID, err := pathIDNamed(c, "docID")
	if err != nil {
		Fail(c, err)
		return
	}
	doc, err := h.svcs.KB.ReindexDoc(ctx, userID(c), id, docID)
	if err != nil {
		Fail(c, err)
		return
	}
	c.JSON(202, envelope{Code: 0, Message: "accepted", Data: doc})
}

func pathIDNamed(c *app.RequestContext, name string) (int64, error) {
	return parseID(c.Param(name))
}
