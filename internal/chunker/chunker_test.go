package chunker

import (
	"strings"
	"testing"
)

func TestExtractBreadcrumbs(t *testing.T) {
	markdown := `---
title: Ignore me
---
# Header 1
Some text
## Subhead 2
More text
### Subhead 3
Even more text
` + "```\n# Not a header\n```" + `
# Header 1B`

	title := "Doc Title"
	contexts := ExtractBreadcrumbs(title, markdown)

	expected := map[int]string{
		3: "Doc Title > Header 1",
		4: "Doc Title > Header 1",
		5: "Doc Title > Header 1 > Subhead 2",
		7: "Doc Title > Header 1 > Subhead 2 > Subhead 3",
		10: "Doc Title > Header 1 > Subhead 2 > Subhead 3", // inside code block
		12: "Doc Title > Header 1B",
	}

	for lineIdx, expectedBc := range expected {
		if contexts[lineIdx].Breadcrumb != expectedBc {
			t.Errorf("Line %d: expected %q, got %q", lineIdx, expectedBc, contexts[lineIdx].Breadcrumb)
		}
	}
}

func TestChunkDocument_Empty(t *testing.T) {
	opts := DefaultChunkingOptions()
	chunks := ChunkDocument("url", "Title", "", opts)
	if len(chunks) != 0 {
		t.Errorf("Expected 0 chunks, got %d", len(chunks))
	}
}

func TestChunkDocument_SlidingWindow(t *testing.T) {
	opts := ChunkingOptions{MaxTokens: 20, Overlap: 5}
	text := "Line 1 is here\nLine 2 is here\nLine 3 is here\nLine 4 is here\nLine 5 is here\nLine 6 is here"
	chunks := ChunkDocument("url", "Title", text, opts)
	
	if len(chunks) < 2 {
		t.Errorf("Expected multiple chunks due to low MaxTokens")
	}
	
	// Check overlap
	foundOverlap := false
	for i := 1; i < len(chunks); i++ {
		prevLines := strings.Split(chunks[i-1].Content, "\n")
		currLines := strings.Split(chunks[i].Content, "\n")
		
		lastPrev := prevLines[len(prevLines)-1]
		// skip the [Context: ...] line in current
		firstCurrIdx := 0
		if strings.HasPrefix(currLines[0], "[Context:") {
			firstCurrIdx = 1
		}
		
		if firstCurrIdx < len(currLines) && lastPrev == currLines[firstCurrIdx] {
			foundOverlap = true
		}
	}
	if !foundOverlap {
		t.Errorf("Expected overlapping chunks")
	}
}

func TestChunkDocument_GiantLine(t *testing.T) {
	opts := ChunkingOptions{MaxTokens: 50, Overlap: 2}
	giantLine := strings.Repeat("a", 300) // 100 chars = 26 tokens approx
	
	chunks := ChunkDocument("url", "Title", giantLine, opts)
	if len(chunks) <= 1 {
		t.Errorf("Expected giant line to be split into multiple chunks")
	}
	for _, c := range chunks {
		if c.Tokens > opts.MaxTokens {
			t.Errorf("Chunk tokens %d exceeds max %d", c.Tokens, opts.MaxTokens)
		}
	}
}
