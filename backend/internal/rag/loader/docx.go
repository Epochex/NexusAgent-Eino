package loader

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

func loadDocx(data []byte) (*LoadedDoc, error) {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open DOCX: %w", err)
	}
	if len(archive.File) > 4096 {
		return nil, fmt.Errorf("DOCX has too many archive entries")
	}
	var document *zip.File
	var total uint64
	media := false
	for _, entry := range archive.File {
		if entry.UncompressedSize64 > 64<<20 || total > 64<<20-entry.UncompressedSize64 {
			return nil, fmt.Errorf("DOCX expanded size exceeds 64 MiB")
		}
		total += entry.UncompressedSize64
		if entry.Name == "word/document.xml" {
			if document != nil {
				return nil, fmt.Errorf("DOCX contains duplicate document XML")
			}
			document = entry
		}
		if strings.HasPrefix(entry.Name, "word/media/") {
			media = true
		}
	}
	if document == nil {
		return nil, fmt.Errorf("word/document.xml not found")
	}
	if document.UncompressedSize64 > MaxTextBytes {
		return nil, fmt.Errorf("DOCX document XML exceeds 16 MiB")
	}
	reader, err := document.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, MaxTextBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxTextBytes {
		return nil, fmt.Errorf("DOCX XML exceeds 16 MiB")
	}
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	var out strings.Builder
	inText := false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode DOCX XML: %w", err)
		}
		switch node := token.(type) {
		case xml.StartElement:
			switch node.Name.Local {
			case "t":
				inText = true
			case "tab":
				out.WriteByte('\t')
			case "br", "cr":
				out.WriteByte('\n')
			}
		case xml.EndElement:
			switch node.Name.Local {
			case "t":
				inText = false
			case "p":
				out.WriteByte('\n')
			case "tc":
				out.WriteByte('\t')
			}
		case xml.CharData:
			if inText {
				out.Write(node)
			}
		}
		if out.Len() > MaxTextBytes {
			return nil, fmt.Errorf("DOCX text exceeds 16 MiB")
		}
	}
	result := loadPlainText([]byte(out.String()))
	if result.Pages[0].Content == "" {
		return nil, fmt.Errorf("DOCX contains no extractable text")
	}
	result.Warnings = append(result.Warnings, "DOCX layout page numbers are not preserved")
	if media {
		result.Warnings = append(result.Warnings, "embedded media was not OCR processed")
	}
	return result, nil
}
