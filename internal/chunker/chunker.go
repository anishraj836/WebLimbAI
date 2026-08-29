package chunker

import (
	"fmt"
	"strings"
)

// rough token estimation: 1 token ~ 4 characters
func estimateTokens(text string) int {
	return (len(text) / 4) + 1
}

func ChunkDocument(docURL string, title string, text string, opts ChunkingOptions) []Chunk {
	if text == "" {
		return []Chunk{}
	}

	contexts := ExtractBreadcrumbs(title, text)
	lines := strings.Split(text, "\n")
	var chunks []Chunk
	chunkIdx := 0

	var currentContent strings.Builder
	currentTokens := 0
	currentBreadcrumb := ""
	if len(contexts) > 0 {
		currentBreadcrumb = contexts[0].Breadcrumb
	}
	
	startLine := 0
	
	flush := func(endLine int) {
		if currentContent.Len() > 0 {
			contentStr := currentContent.String()
			
			bcStr := ""
			if currentBreadcrumb != "" {
				bcStr = fmt.Sprintf("[Context: %s]\n", currentBreadcrumb)
			}
			finalContent := bcStr + contentStr
			
			chunks = append(chunks, Chunk{
				ID:          fmt.Sprintf("%s#chunk%d", docURL, chunkIdx),
				DocURL:      docURL,
				ChunkIndex:  chunkIdx,
				Breadcrumb:  currentBreadcrumb,
				Content:     finalContent,
				Tokens:      estimateTokens(finalContent),
				StartOffset: startLine,
				EndOffset:   endLine,
			})
			chunkIdx++
			currentContent.Reset()
			currentTokens = 0
		}
	}

	i := 0
	for i < len(lines) {
		line := lines[i]
		lineTokens := estimateTokens(line)
		
		if currentTokens+lineTokens > opts.MaxTokens && currentTokens > 0 {
			// Window full, flush
			flush(i)
			
			// Handle overlap: back up a few lines
			overlapTokens := 0
			backIdx := i - 1
			for backIdx >= startLine {
				tokens := estimateTokens(lines[backIdx])
				if overlapTokens+tokens > opts.Overlap && overlapTokens > 0 {
					break
				}
				overlapTokens += tokens
				backIdx--
			}
			backIdx++ // first line of overlap
			
			if backIdx <= startLine {
				if i == startLine {
				    i++
				}
				startLine = i
			} else {
				startLine = backIdx
				i = backIdx
			}
			
			if len(contexts) > startLine {
				currentBreadcrumb = contexts[startLine].Breadcrumb
			}
			continue
		}
		
		if lineTokens > opts.MaxTokens {
			// giant line
			if currentTokens > 0 {
				flush(i)
			}
			
			// Split giant line
			runes := []rune(line)
			bc := contexts[i].Breadcrumb
			bcStr := ""
			if bc != "" {
				bcStr = "[Context: " + bc + "]\n"
			}
			bcTokens := estimateTokens(bcStr)
			
			availTokens := opts.MaxTokens - bcTokens - 1
			if availTokens <= 0 {
				availTokens = 1
			}
			chunkSize := availTokens * 4 // roughly
			for start := 0; start < len(runes); start += chunkSize {
				end := start + chunkSize
				if end > len(runes) {
					end = len(runes)
				}
				subline := string(runes[start:end])
				

				finalContent := bcStr + subline
				
				chunks = append(chunks, Chunk{
					ID:          fmt.Sprintf("%s#chunk%d", docURL, chunkIdx),
					DocURL:      docURL,
					ChunkIndex:  chunkIdx,
					Breadcrumb:  bc,
					Content:     finalContent,
					Tokens:      estimateTokens(finalContent),
					StartOffset: i,
					EndOffset:   i, // Single line, so end offset is same as start
				})
				chunkIdx++
			}
			i++
			startLine = i
			continue
		}
		
		if currentTokens == 0 {
			startLine = i
			currentBreadcrumb = contexts[i].Breadcrumb
		}
		if currentTokens > 0 {
			currentContent.WriteString("\n")
		}
		currentContent.WriteString(line)
		currentTokens += lineTokens
		i++
	}
	
	flush(len(lines))

	return chunks
}
