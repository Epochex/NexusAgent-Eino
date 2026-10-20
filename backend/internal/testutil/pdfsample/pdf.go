// Package pdfsample creates small deterministic PDF fixtures without external
// documents, credentials, or a runtime Python/Office dependency.
package pdfsample

import (
	"bytes"
	"fmt"
	"strings"
)

func Simple(text string) []byte {
	text = strings.NewReplacer("\\", "\\\\", "(", "\\(", ")", "\\)").Replace(text)
	return document([]byte("BT /F1 12 Tf 10 20 Td ("+text+") Tj ET"), false)
}

func document(content []byte, compressed bool) []byte {
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	objects := []string{"<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 500 500] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>", "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"}
	offsets := []int{0}
	for i, obj := range objects {
		offsets = append(offsets, out.Len())
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, obj)
	}
	offsets = append(offsets, out.Len())
	filter := ""
	if compressed {
		filter = " /Filter /FlateDecode"
	}
	fmt.Fprintf(&out, "5 0 obj\n<< /Length %d%s >>\nstream\n", len(content), filter)
	out.Write(content)
	out.WriteString("\nendstream\nendobj\n")
	xref := out.Len()
	out.WriteString("xref\n0 6\n0000000000 65535 f \n")
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", xref)
	return out.Bytes()
}
