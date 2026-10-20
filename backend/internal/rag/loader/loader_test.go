package loader

import (
	"archive/zip"
	"bytes"
	"errors"
	"strings"
	"testing"
)

func documentZip(t *testing.T, xml string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	part, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = part.Write([]byte(xml)); err != nil {
		t.Fatal(err)
	}
	if err = zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
func TestHTMLExtractsVisibleText(t *testing.T) {
	doc, err := Load("report.html", []byte(`<html><head><style>hidden css</style><script>hidden js</script></head><body><h1>标题</h1><p>A &amp; B<br>第二行</p><script>hidden tracking</script></body></html>`))
	if err != nil {
		t.Fatal(err)
	}
	text := doc.Pages[0].Content
	if strings.Contains(text, "<") || strings.Contains(text, "hidden") || !strings.Contains(text, "A & B") || !strings.Contains(text, "标题") {
		t.Fatalf("HTML was indexed as markup/scripts: %s", text)
	}
}
func TestDOCXPreservesRunsEntitiesAndParagraphs(t *testing.T) {
	raw := documentZip(t, `<?xml version="1.0"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>Hello</w:t></w:r><w:r><w:t xml:space="preserve"> world &amp; Co.</w:t></w:r></w:p><w:p><w:r><w:t>第二段</w:t><w:tab/><w:t>表格</w:t></w:r></w:p></w:body></w:document>`)
	doc, err := Load("report.docx", raw)
	if err != nil {
		t.Fatal(err)
	}
	if text := doc.Pages[0].Content; text != "Hello world & Co.\n第二段\t表格" {
		t.Fatalf("corrupted DOCX text: %q", text)
	}
}
func TestPlainTextDoesNotInventPages(t *testing.T) {
	text := strings.Repeat("中文内容", 1000)
	doc, err := Load("report.txt", []byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Pages) != 1 || doc.Pages[0].Content != text {
		t.Fatal("plain text citations invent page numbers or lose content")
	}
}

func TestDOCXRejectsExpandedXMLAndMalformedContent(t *testing.T) {
	huge := documentZip(t, strings.Repeat("x", MaxTextBytes+1))
	if _, err := Load("large.docx", huge); err == nil {
		t.Fatal("compressed DOCX bypassed expansion bound")
	}
	malformed := documentZip(t, `<w:document xmlns:w="urn:word"><w:p><w:t>broken`)
	if _, err := Load("broken.docx", malformed); err == nil {
		t.Fatal("malformed XML silently indexed")
	}
}
func TestPDFPageFailuresRemainVisible(t *testing.T) {
	doc, err := loadPDFPages(3, func(page int) (string, error) {
		switch page {
		case 1:
			return "source text", nil
		case 2:
			return "", errors.New("bad page")
		default:
			return "", nil
		}
	})
	if err != nil || len(doc.Pages) != 1 || doc.Pages[0].Page != 1 || len(doc.Warnings) != 2 {
		t.Fatalf("silent partial extraction %+v %v", doc, err)
	}
	if _, err = loadPDFPages(1, func(int) (string, error) { return "", nil }); err == nil {
		t.Fatal("scan-only PDF reported success")
	}
}
func TestLoaderRejectsInvalidTextEncoding(t *testing.T) {
	for _, name := range []string{"invalid.txt", "invalid.html"} {
		if _, err := Load(name, []byte{0xff, 0xfe, 0x81}); err == nil {
			t.Fatal("invalid bytes silently replaced")
		}
	}
}
