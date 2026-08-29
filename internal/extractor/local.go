package extractor

import (
	"bytes"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// IsTextFile checks if file data is clean UTF-8 or standard text MIME type, safely handling data < 512 bytes.
func IsTextFile(path string, data []byte) bool {
	if len(data) == 0 {
		return false
	}
	// Check mime type using http.DetectContentType
	sampleSize := len(data)
	if sampleSize > 512 {
		sampleSize = 512
	}
	mimeType := http.DetectContentType(data[:sampleSize])
	if strings.HasPrefix(mimeType, "text/") {
		return true
	}
	if mimeType == "application/json" {
		return true
	}

	// Fallback to utf8 valid check for the sample
	if utf8.Valid(data[:sampleSize]) {
		// Check for null bytes which usually indicate binary
		if bytes.IndexByte(data[:sampleSize], 0) != -1 {
			return false
		}
		return true
	}
	return false
}

// ExtractLocalFile extracts text, title, and token count from a local file.
func ExtractLocalFile(path string, data []byte) (title string, cleanBody string, totalTokens int, err error) {
	if len(data) == 0 {
		return "", "", 0, errors.New("empty file")
	}

	ext := strings.ToLower(filepath.Ext(path))
	baseName := filepath.Base(path)
	if ext != "" {
		baseName = strings.TrimSuffix(baseName, ext)
	}

	switch ext {
	case ".md", ".markdown":
		lines := strings.Split(string(data), "\n")
		title = baseName
		for _, line := range lines {
			if strings.HasPrefix(line, "# ") {
				title = strings.TrimSpace(strings.TrimPrefix(line, "# "))
				break
			}
		}
		cleanBody = string(data)
		totalTokens = CountBPETokens(cleanBody)
		return title, cleanBody, totalTokens, nil

	case ".html", ".htm":
		cleanBody, totalTokens, title = ConvertHTMLToMarkdown("file://"+path, data, "clean_rag")
		if title == "" {
			title = baseName
		}
		return title, cleanBody, totalTokens, nil

	case ".pdf":
		text, pdfTitle, pdfErr := ExtractTextFromPDF(data)
		if pdfErr != nil {
			return "", "", 0, pdfErr
		}
		cleanBody = text
		if pdfTitle != "" {
			title = pdfTitle
		} else {
			title = baseName
		}
		totalTokens = CountBPETokens(cleanBody)
		return title, cleanBody, totalTokens, nil

	case ".txt", ".json", ".yaml", ".yml", ".rst", ".csv":
		fallthrough
	default:
		if !IsTextFile(path, data) {
			return "", "", 0, errors.New("unsupported binary format")
		}
		
		cleanBody = string(data)
		lines := strings.SplitN(cleanBody, "\n", 2)
		title = baseName
		if len(lines) > 0 && len(strings.TrimSpace(lines[0])) > 0 && len(strings.TrimSpace(lines[0])) < 100 {
			// use first line as title if it's short
			// title = strings.TrimSpace(lines[0])
		}
		totalTokens = CountBPETokens(cleanBody)
		return title, cleanBody, totalTokens, nil
	}
}
