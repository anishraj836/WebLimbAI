package chunker

import (
	"strings"
)

type LineContext struct {
	LineNumber int
	Breadcrumb string
}

func ExtractBreadcrumbs(title string, markdown string) []LineContext {
	lines := strings.Split(markdown, "\n")
	contexts := make([]LineContext, len(lines))

	stack := []string{}
	if title != "" {
		stack = append(stack, title)
	}

	inCodeBlock := false
	inFrontmatter := false

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)

		if i == 0 && strings.HasPrefix(trimmed, "---") {
			inFrontmatter = true
			contexts[i] = LineContext{LineNumber: i, Breadcrumb: strings.Join(stack, " > ")}
			continue
		}
		if inFrontmatter {
			if strings.HasPrefix(trimmed, "---") {
				inFrontmatter = false
			}
			contexts[i] = LineContext{LineNumber: i, Breadcrumb: strings.Join(stack, " > ")}
			continue
		}

		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inCodeBlock = !inCodeBlock
			contexts[i] = LineContext{LineNumber: i, Breadcrumb: strings.Join(stack, " > ")}
			continue
		}

		if !inCodeBlock && strings.HasPrefix(trimmed, "#") {
			parts := strings.SplitN(trimmed, " ", 2)
			if len(parts) == 2 && strings.Count(parts[0], "#") == len(parts[0]) {
				level := len(parts[0])
				heading := strings.TrimSpace(parts[1])

				actualLevel := level
				if title != "" {
					actualLevel += 1
				}
				
				if actualLevel <= len(stack) {
					stack = stack[:actualLevel-1]
				}
				for len(stack) < actualLevel-1 {
					stack = append(stack, "")
				}
				stack = append(stack, heading)
			}
		}

		bc := strings.Join(stack, " > ")
		bc = strings.ReplaceAll(bc, " >  > ", " > ")
		bc = strings.TrimSuffix(bc, " > ")
		
		contexts[i] = LineContext{LineNumber: i, Breadcrumb: bc}
	}

	return contexts
}
