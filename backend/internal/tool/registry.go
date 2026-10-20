// 内置工具注册表（td.md §8.7）。
// 注册表模式：新增内置工具 = 写一个 handler + Register 一行，
// Ep 04 教学点：工具描述（description/JSON Schema）质量直接决定模型调用准确率。
package tool

import (
	"sort"
	"strings"
	"sync"
)

// registry 内置工具注册表实现。
type registry struct {
	mu       sync.RWMutex
	handlers map[string]BuiltinHandler
	prefixes []prefixHandler
}
type prefixHandler struct {
	prefix  string
	factory func(string) (BuiltinHandler, bool)
}

func (r *registry) Unregister(code string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, code)
}
func (r *registry) RegisterPrefix(prefix string, factory func(string) (BuiltinHandler, bool)) {
	if prefix == "" || factory == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.prefixes {
		if r.prefixes[i].prefix == prefix {
			r.prefixes[i].factory = factory
			return
		}
	}
	r.prefixes = append(r.prefixes, prefixHandler{prefix, factory})
	sort.Slice(r.prefixes, func(i, j int) bool {
		if len(r.prefixes[i].prefix) != len(r.prefixes[j].prefix) {
			return len(r.prefixes[i].prefix) > len(r.prefixes[j].prefix)
		}
		return r.prefixes[i].prefix < r.prefixes[j].prefix
	})
}

// NewRegistry 构造空注册表。
func NewRegistry() Registry {
	return &registry{handlers: map[string]BuiltinHandler{}}
}

// Register 注册（同 code 覆盖，便于测试替换）。
func (r *registry) Register(code string, h BuiltinHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[code] = h
}

// Get 查找。
func (r *registry) Get(code string) (BuiltinHandler, bool) {
	r.mu.RLock()
	h, ok := r.handlers[code]
	if ok {
		r.mu.RUnlock()
		return h, true
	}
	prefixes := append([]prefixHandler(nil), r.prefixes...)
	r.mu.RUnlock()
	for _, entry := range prefixes {
		if strings.HasPrefix(code, entry.prefix) {
			if handler, ok := entry.factory(code); ok {
				return handler, true
			}
		}
	}
	return nil, false
}

// Codes 列出全部已注册 code（排序，稳定输出）。
func (r *registry) Codes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	codes := make([]string, 0, len(r.handlers))
	for c := range r.handlers {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	return codes
}
