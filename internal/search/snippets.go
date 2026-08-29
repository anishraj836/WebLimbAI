package search

import (
	"strings"
	"regexp"
)

func Generate3LineSnippet(content string, queryTerms []string, maxTokens int) string {
	lines := strings.Split(content, "\n")
	
	bestIdx := 0
	maxScore := -1
	
	for i, line := range lines {
		lowerLine := strings.ToLower(line)
		score := 0
		for _, term := range queryTerms {
			if term == "" { continue }
			if strings.Contains(lowerLine, strings.ToLower(term)) {
				score++
			}
		}
		if score > maxScore {
			maxScore = score
			bestIdx = i
		}
	}
	
	start := bestIdx - 1
	if start < 0 { start = 0 }
	end := bestIdx + 1
	if end >= len(lines) { end = len(lines) - 1 }
	
	var snippetLines []string
	for i := start; i <= end; i++ {
		line := lines[i]
		for _, term := range queryTerms {
			if term == "" { continue }
			re := regexp.MustCompile("(?i)\\b" + regexp.QuoteMeta(term) + "\\b")
			line = re.ReplaceAllStringFunc(line, func(match string) string {
				return "<b>" + match + "</b>"
			})
		}
		
		// Clean up nested <b> tags if they occurred
		line = strings.ReplaceAll(line, "<b><b>", "<b>")
		line = strings.ReplaceAll(line, "</b></b>", "</b>")
		
		snippetLines = append(snippetLines, line)
	}
	
	return strings.Join(snippetLines, "\n")
}