// Package loader extracts bounded document text and explicit parsing warnings.
package loader

import (
	"bytes"
	"fmt"
	pdf "github.com/dslipak/pdf"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const MaxDocumentBytes = 32 << 20
const MaxTextBytes = 16 << 20
const MaxPDFPages = 2000

type LoadedDoc struct {
	Pages    []PageContent
	Warnings []string
}
type PageContent struct {
	Page    int
	Content string
}

func Load(filename string, data []byte) (*LoadedDoc, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty document")
	}
	if len(data) > MaxDocumentBytes {
		return nil, fmt.Errorf("document exceeds 32 MiB")
	}
	switch strings.ToLower(filepath.Ext(filename)) {
	case ".pdf":
		return loadPDF(data)
	case ".docx":
		return loadDocx(data)
	case ".html", ".htm":
		return loadHTML(data)
	case ".md", ".markdown", ".txt", "":
		if !utf8.Valid(data) {
			return nil, fmt.Errorf("text encoding must be UTF-8")
		}
		if len(data) > MaxTextBytes {
			return nil, fmt.Errorf("text exceeds 16 MiB")
		}
		return loadPlainText(data), nil
	default:
		return nil, fmt.Errorf("unsupported file type: %s", filepath.Ext(filename))
	}
}
func loadPlainText(data []byte) *LoadedDoc {
	return &LoadedDoc{Pages: []PageContent{{Page: 1, Content: normalizeText(string(data))}}}
}
func loadPDF(data []byte) (*LoadedDoc, error) {
	doc, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open PDF: %w", err)
	}
	return loadPDFPages(doc.NumPage(), func(page int) (text string, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("page parser failed")
			}
		}()
		return doc.Page(page).GetPlainText(nil)
	})
}
func loadPDFPages(count int, read func(int) (string, error)) (*LoadedDoc, error) {
	if count < 1 || count > MaxPDFPages {
		return nil, fmt.Errorf("PDF page count outside 1..2000")
	}
	result := &LoadedDoc{}
	total := 0
	for page := 1; page <= count; page++ {
		text, err := read(page)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("page %d could not be extracted", page))
			continue
		}
		content := normalizeText(text)
		if content == "" {
			result.Warnings = append(result.Warnings, fmt.Sprintf("page %d has no extractable text; OCR was not performed", page))
			continue
		}
		total += len(content)
		if total > MaxTextBytes {
			return nil, fmt.Errorf("extracted PDF text exceeds 16 MiB")
		}
		result.Pages = append(result.Pages, PageContent{Page: page, Content: content})
	}
	if len(result.Pages) == 0 {
		return nil, fmt.Errorf("no PDF text extracted; OCR may be required")
	}
	return result, nil
}
func normalizeText(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	// One linear pass avoids repeated full copies of documents with many blank lines.
	var out strings.Builder
	out.Grow(len(text))
	newlines := 0
	for _, r := range text {
		if r == '\n' {
			newlines++
			if newlines > 2 {
				continue
			}
		} else {
			newlines = 0
		}
		out.WriteRune(r)
	}
	return strings.TrimSpace(out.String())
}
