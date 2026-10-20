package loader

import (
	"bytes"
	"fmt"
	"golang.org/x/net/html"
	"io"
	"strings"
	"unicode/utf8"
)

func loadHTML(data []byte) (*LoadedDoc, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("HTML encoding must be UTF-8")
	}
	tokenizer := html.NewTokenizer(bytes.NewReader(data))
	var out strings.Builder
	hidden := 0
	for {
		kind := tokenizer.Next()
		if kind == html.ErrorToken {
			if err := tokenizer.Err(); err != io.EOF {
				return nil, err
			}
			break
		}
		switch kind {
		case html.StartTagToken, html.SelfClosingTagToken, html.EndTagToken:
			name, _ := tokenizer.TagName()
			tag := string(name)
			switch tag {
			case "script", "style", "noscript", "template":
				if kind == html.EndTagToken {
					if hidden > 0 {
						hidden--
					}
				} else if kind != html.SelfClosingTagToken {
					hidden++
				}
				continue
			}
			if hidden > 0 {
				continue
			}
			switch tag {
			case "p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article":
				out.WriteByte('\n')
			}
		case html.TextToken:
			if hidden == 0 {
				out.Write(tokenizer.Text())
			}
		}
		if out.Len() > MaxTextBytes {
			return nil, fmt.Errorf("HTML text exceeds 16 MiB")
		}
	}
	result := loadPlainText([]byte(out.String()))
	if result.Pages[0].Content == "" {
		return nil, fmt.Errorf("HTML has no visible text")
	}
	return result, nil
}
