package extractor

import (
	"log"
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	tiktoken "github.com/pkoukk/tiktoken-go"
	"golang.org/x/net/html"
)

var (
	globalTiktokenCodec *tiktoken.Tiktoken
	globalTiktokenOnce  sync.Once
	globalTiktokenErr   error
)

func getGlobalTiktokenCodec() (*tiktoken.Tiktoken, error) {
	globalTiktokenOnce.Do(func() {
		globalTiktokenCodec, globalTiktokenErr = tiktoken.GetEncoding("cl100k_base")
		if globalTiktokenErr != nil {
			log.Printf("[extractor] WARNING: cl100k_base tokenizer unavailable (%v); "+
				"CountBPETokens is falling back to whitespace word counts, so token "+
				"numbers are NOT BPE tokens", globalTiktokenErr)
		}
	})
	return globalTiktokenCodec, globalTiktokenErr
}

// TokenizerStatus reports whether CountBPETokens is using the real
// cl100k_base BPE tokenizer. A non-nil error means counts are word counts,
// so any token-savings figure measured in that state is not valid.
func TokenizerStatus() error {
	_, err := getGlobalTiktokenCodec()
	return err
}

// CleanDocument represents a structured document after HTML boilerplate removal.
type CleanDocument struct {
	URL       string   `json:"url"`
	Title     string   `json:"title"`
	Body      string   `json:"body"`
	Language  string   `json:"language"`
	Timestamp string   `json:"timestamp"`
	Links     []string `json:"links"`
}

// ConvertHTMLToMarkdown converts raw HTML AST into token-efficient Markdown using mode ("clean_rag", "preserve_links", "raw").
func ConvertHTMLToMarkdown(sourceURL string, htmlBytes []byte, mode string) (markdownText string, tokenEstimate int, title string) {
	if mode == "" {
		mode = "clean_rag"
	}

	if IsPDF(htmlBytes) {
		text, pdfTitle, err := ExtractTextFromPDF(htmlBytes)
		if err != nil {
			errText := fmt.Sprintf("# PDF Extraction Error\n\n%v", err)
			return errText, CountBPETokens(errText), sourceURL
		}
		if pdfTitle == "" {
			pdfTitle = sourceURL
		}
		md := text
		if !strings.HasPrefix(strings.TrimSpace(md), "# ") {
			md = fmt.Sprintf("# %s\n\n%s", pdfTitle, md)
		}
		return md, CountBPETokens(md), pdfTitle
	}

	doc, err := html.Parse(bytes.NewReader(htmlBytes))
	if err != nil {
		return string(htmlBytes), CountBPETokens(string(htmlBytes)), sourceURL
	}

	title = ExtractTitleFromAST(doc)
	if title == "" {
		title = sourceURL
	}

	var baseURL *url.URL
	if sourceURL != "" {
		baseURL, _ = url.Parse(sourceURL)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("# %s\n\n", title))

	processASTTables(doc, &sb, 0)
	renderASTBody(doc, &sb, baseURL, mode, 0)

	result := strings.TrimSpace(sb.String())
	tokenEstimate = CountBPETokens(result)
	return result, tokenEstimate, title
}

// ExtractTitleFromAST extracts document title from <title> or <h1> tag using AST traversal.
func ExtractTitleFromAST(n *html.Node) string {
	if n == nil {
		return ""
	}
	var title string
	var findTitle func(*html.Node, int)
	findTitle = func(node *html.Node, depth int) {
		if node == nil || depth > 128 {
			return
		}
		if node.Type == html.ElementNode && strings.ToLower(node.Data) == "title" {
			title = strings.TrimSpace(getNodeText(node, depth+1))
			return
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			findTitle(c, depth+1)
			if title != "" {
				return
			}
		}
	}
	findTitle(n, 0)

	if title == "" {
		var findH1 func(*html.Node, int)
		findH1 = func(node *html.Node, depth int) {
			if node == nil || depth > 128 {
				return
			}
			if node.Type == html.ElementNode && strings.ToLower(node.Data) == "h1" {
				title = strings.TrimSpace(getNodeText(node, depth+1))
				return
			}
			for c := node.FirstChild; c != nil; c = c.NextSibling {
				findH1(c, depth+1)
				if title != "" {
					return
				}
			}
		}
		findH1(n, 0)
	}

	return title
}

// CountBPETokens calculates exact Byte-Pair Encoding subword tokens using tiktoken.
func CountBPETokens(text string) (count int) {
	if text == "" {
		return 0
	}
	defer func() {
		if r := recover(); r != nil {
			count = len(strings.Fields(text))
		}
	}()
	tke, err := getGlobalTiktokenCodec()
	if err != nil {
		return len(strings.Fields(text))
	}
	allowedSpecial := []string{"<|endoftext|>", "<|im_start|>", "<|im_end|>", "<|fim_prefix|>", "<|fim_middle|>", "<|fim_suffix|>"}
	tokens := tke.Encode(text, allowedSpecial, nil)
	return len(tokens)
}

// ProcessRawHTML parses HTML DOM AST, removes non-content nodes, and returns a CleanDocument.
func ProcessRawHTML(sourceURL string, htmlContent []byte) (*CleanDocument, error) {
	doc, err := html.Parse(bytes.NewReader(htmlContent))
	if err != nil {
		return nil, err
	}

	title := ExtractTitleFromAST(doc)
	lang := extractAttributeFromAST(doc, "html", "lang", 0)
	if lang == "" {
		lang = "en"
	}

	pubdate := extractMetaContentFromAST(doc, "pubdate", 0)
	links := extractLinksFromAST(doc, sourceURL)

	bodyParts := extractBlockTextsFromAST(doc)
	cleanBody := strings.Join(bodyParts, " ")

	return &CleanDocument{
		URL:       sourceURL,
		Title:     title,
		Body:      cleanBody,
		Language:  lang,
		Timestamp: pubdate,
		Links:     links,
	}, nil
}

// SerializeJSON marshals CleanDocument to JSON.
func (c *CleanDocument) SerializeJSON() ([]byte, error) {
	return json.Marshal(c)
}

// ExtractFields extracts structured key-value pairs from Markdown text based on field schemas.
func ExtractFields(markdownText string, fields []string) map[string]string {
	result := make(map[string]string)
	lines := strings.Split(markdownText, "\n")

	for _, field := range fields {
		fLower := strings.ToLower(field)
		for _, line := range lines {
			lineLower := strings.ToLower(line)
			if strings.Contains(lineLower, fLower) {
				cleanLine := strings.TrimSpace(line)
				cleanLine = strings.TrimPrefix(cleanLine, "#")
				cleanLine = strings.TrimPrefix(cleanLine, "-")
				cleanLine = strings.TrimSpace(cleanLine)
				result[field] = cleanLine
				break
			}
		}
	}

	return result
}

// Helper functions for AST Traversal

func isIgnoredNode(n *html.Node, mode string) bool {
	if n == nil || n.Type != html.ElementNode {
		return false
	}
	tag := strings.ToLower(n.Data)
	if tag == "header" {
		return isTopLevelPageHeader(n)
	}
	ignored := map[string]bool{
		"script": true, "style": true, "noscript": true, "iframe": true,
		"nav": true, "footer": true,
	}
	if mode != "raw" {
		ignored["form"] = true
		ignored["svg"] = true
	}
	return ignored[tag]
}

func isTopLevelPageHeader(n *html.Node) bool {
	p := n.Parent
	for p != nil {
		pTag := strings.ToLower(p.Data)
		if pTag == "main" || pTag == "article" || pTag == "section" {
			return false
		}
		p = p.Parent
	}
	return true
}

func getNodeText(n *html.Node, depth int) string {
	if n == nil || depth > 128 {
		return ""
	}
	if n.Type == html.TextNode {
		return n.Data
	}
	var sb strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		sb.WriteString(getNodeText(c, depth+1))
	}
	return sb.String()
}

func extractTextFromAST(n *html.Node, mode string, depth int) string {
	if n == nil || depth > 128 || isIgnoredNode(n, mode) {
		return ""
	}
	if n.Type == html.TextNode {
		return n.Data
	}
	var parts []string
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		t := strings.TrimSpace(extractTextFromAST(c, mode, depth+1))
		if t != "" {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, " ")
}

func formatASTInlineText(n *html.Node, baseURL *url.URL, mode string, depth int) string {
	if n == nil || depth > 128 {
		return ""
	}
	if n.Type == html.TextNode {
		return n.Data
	}
	if isIgnoredNode(n, mode) {
		return ""
	}

	tag := strings.ToLower(n.Data)
	if tag == "code" {
		t := strings.TrimSpace(getNodeText(n, depth+1))
		if t != "" {
			return fmt.Sprintf("`%s`", t)
		}
		return ""
	}

	if tag == "a" {
		var href string
		for _, attr := range n.Attr {
			if strings.ToLower(attr.Key) == "href" {
				href = attr.Val
				break
			}
		}
		anchorText := strings.TrimSpace(getNodeText(n, depth+1))
		if href != "" && anchorText != "" {
			if strings.HasPrefix(anchorText, "[") && strings.Contains(anchorText, "](") {
				return anchorText
			}
			if mode == "clean_rag" && strings.HasPrefix(href, "#") {
				return anchorText
			}
			parsed, err := url.Parse(href)
			if err != nil {
				return anchorText
			}
			resolved := parsed
			if baseURL != nil {
				resolved = baseURL.ResolveReference(parsed)
			}
			if resolved.Scheme != "http" && resolved.Scheme != "https" {
				return anchorText
			}
			return fmt.Sprintf("[%s](%s)", anchorText, resolved.String())
		}
		return anchorText
	}

	var sb strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		sb.WriteString(formatASTInlineText(c, baseURL, mode, depth+1))
	}
	return sb.String()
}

func hasBlockChildren(n *html.Node) bool {
	if n == nil {
		return false
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode {
			tag := strings.ToLower(c.Data)
			switch tag {
			case "h1", "h2", "h3", "h4", "h5", "h6", "p", "ul", "ol", "table", "pre", "blockquote", "div", "section", "article", "header", "footer", "main", "nav", "aside":
				return true
			}
		}
	}
	return false
}

func renderASTBody(n *html.Node, sb *strings.Builder, baseURL *url.URL, mode string, depth int) {
	if n == nil || depth > 128 {
		return
	}

	if n.Type == html.TextNode {
		t := strings.TrimSpace(n.Data)
		if t != "" && n.Parent != nil {
			pTag := strings.ToLower(n.Parent.Data)
			switch pTag {
			case "h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "blockquote", "pre", "a", "code", "table", "th", "td", "script", "style", "title", "head":
				return
			default:
				sb.WriteString(fmt.Sprintf("%s\n\n", t))
				return
			}
		}
		return
	}

	if n.Type == html.ElementNode {
		if isIgnoredNode(n, mode) {
			return
		}

		tag := strings.ToLower(n.Data)
		if tag == "table" {
			return
		}
		switch tag {
		case "h1":
			t := strings.TrimSpace(getNodeText(n, depth+1))
			if t != "" {
				sb.WriteString(fmt.Sprintf("# %s\n\n", t))
			}
			return
		case "h2":
			t := strings.TrimSpace(getNodeText(n, depth+1))
			if t != "" {
				sb.WriteString(fmt.Sprintf("## %s\n\n", t))
			}
			return
		case "h3":
			t := strings.TrimSpace(getNodeText(n, depth+1))
			if t != "" {
				sb.WriteString(fmt.Sprintf("### %s\n\n", t))
			}
			return
		case "h4", "h5", "h6":
			t := strings.TrimSpace(getNodeText(n, depth+1))
			if t != "" {
				sb.WriteString(fmt.Sprintf("#### %s\n\n", t))
			}
			return
		case "p":
			t := strings.TrimSpace(formatASTInlineText(n, baseURL, mode, depth+1))
			if t != "" {
				sb.WriteString(fmt.Sprintf("%s\n\n", t))
			}
			return
		case "ul", "ol":
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && strings.ToLower(c.Data) == "li" {
					itemText := strings.TrimSpace(formatASTInlineText(c, baseURL, mode, depth+1))
					if itemText != "" {
						sb.WriteString(fmt.Sprintf("- %s\n", itemText))
					}
				}
			}
			sb.WriteString("\n")
			return
		case "blockquote":
			t := strings.TrimSpace(getNodeText(n, depth+1))
			if t != "" {
				sb.WriteString(fmt.Sprintf("> %s\n\n", t))
			}
			return
		case "pre":
			t := strings.TrimSpace(getNodeText(n, depth+1))
			if t != "" {
				sb.WriteString(fmt.Sprintf("```\n%s\n```\n\n", t))
			}
			return
		case "div", "section", "article", "span", "aside":
			if !hasBlockChildren(n) {
				t := strings.TrimSpace(formatASTInlineText(n, baseURL, mode, depth+1))
				if t != "" {
					sb.WriteString(fmt.Sprintf("%s\n\n", t))
					return
				}
			}
		}
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		renderASTBody(c, sb, baseURL, mode, depth+1)
	}
}

func processASTTables(n *html.Node, sb *strings.Builder, depth int) {
	if n == nil || depth > 128 {
		return
	}
	if n.Type == html.ElementNode && strings.ToLower(n.Data) == "table" {
		var headers []string
		var rows [][]string

		var extractRows func(*html.Node, int)
		extractRows = func(node *html.Node, d int) {
			if node == nil || d > 128 {
				return
			}
			if node != n && node.Type == html.ElementNode && strings.ToLower(node.Data) == "table" {
				return
			}
			if node.Type == html.ElementNode {
				tag := strings.ToLower(node.Data)
				if tag == "th" {
					if len(headers) < 50 {
						cellText := strings.TrimSpace(getNodeText(node, d+1))
						if len(cellText) > 1000 {
							cellText = cellText[:1000]
						}
						headers = append(headers, cellText)
					}
				} else if tag == "tr" {
					if len(rows) < 500 {
						var row []string
						for c := node.FirstChild; c != nil; c = c.NextSibling {
							if c.Type == html.ElementNode && strings.ToLower(c.Data) == "td" {
								if len(row) < 50 {
									cellText := strings.TrimSpace(getNodeText(c, d+1))
									if len(cellText) > 1000 {
										cellText = cellText[:1000]
									}
									row = append(row, cellText)
								}
							}
						}
						if len(row) > 0 {
							rows = append(rows, row)
						}
					}
				}
			}
			for c := node.FirstChild; c != nil; c = c.NextSibling {
				extractRows(c, d+1)
			}
		}

		extractRows(n, depth+1)

		if len(headers) == 0 && len(rows) > 0 {
			maxCols := len(rows[0])
			if maxCols > 50 {
				maxCols = 50
			}
			for colIdx := 0; colIdx < maxCols; colIdx++ {
				headers = append(headers, fmt.Sprintf("Col %d", colIdx+1))
			}
		}

		if len(headers) > 50 {
			headers = headers[:50]
		}

		if len(headers) > 0 {
			sb.WriteString("| " + strings.Join(headers, " | ") + " |\n")
			dividers := make([]string, len(headers))
			for dIdx := range dividers {
				dividers[dIdx] = "---"
			}
			sb.WriteString("| " + strings.Join(dividers, " | ") + " |\n")
			for _, r := range rows {
				if len(r) > len(headers) {
					r = r[:len(headers)]
				}
				for len(r) < len(headers) {
					r = append(r, "")
				}
				sb.WriteString("| " + strings.Join(r, " | ") + " |\n")
			}
			sb.WriteString("\n")
		}
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		processASTTables(c, sb, depth+1)
	}
}

func extractAttributeFromAST(n *html.Node, targetTag, attrKey string, depth int) string {
	if n == nil || depth > 128 {
		return ""
	}
	if n.Type == html.ElementNode && strings.ToLower(n.Data) == strings.ToLower(targetTag) {
		for _, a := range n.Attr {
			if strings.ToLower(a.Key) == strings.ToLower(attrKey) {
				return a.Val
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		val := extractAttributeFromAST(c, targetTag, attrKey, depth+1)
		if val != "" {
			return val
		}
	}
	return ""
}

func extractMetaContentFromAST(n *html.Node, name string, depth int) string {
	if n == nil || depth > 128 {
		return ""
	}
	if n.Type == html.ElementNode && strings.ToLower(n.Data) == "meta" {
		var matchedName bool
		var content string
		for _, a := range n.Attr {
			if strings.ToLower(a.Key) == "name" && strings.ToLower(a.Val) == strings.ToLower(name) {
				matchedName = true
			}
			if strings.ToLower(a.Key) == "content" {
				content = a.Val
			}
		}
		if matchedName {
			return content
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		val := extractMetaContentFromAST(c, name, depth+1)
		if val != "" {
			return val
		}
	}
	return ""
}

func extractLinksFromAST(n *html.Node, sourceURL string) []string {
	var links []string
	baseURL, _ := url.Parse(sourceURL)

	var walk func(*html.Node, int)
	walk = func(node *html.Node, depth int) {
		if node == nil || depth > 128 {
			return
		}
		if node.Type == html.ElementNode && strings.ToLower(node.Data) == "a" {
			for _, a := range node.Attr {
				if strings.ToLower(a.Key) == "href" {
					if parsed, err := url.Parse(a.Val); err == nil && baseURL != nil {
						parsed.Fragment = ""
						abs := baseURL.ResolveReference(parsed)
						if abs.Scheme == "http" || abs.Scheme == "https" {
							links = append(links, abs.String())
						}
					}
					break
				}
			}
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			walk(c, depth+1)
		}
	}
	walk(n, 0)
	return links
}

func extractBlockTextsFromAST(n *html.Node) []string {
	var parts []string
	var walk func(*html.Node, int)
	walk = func(node *html.Node, depth int) {
		if node == nil || depth > 128 {
			return
		}
		if isIgnoredNode(node, "clean_rag") {
			return
		}
		if node.Type == html.ElementNode {
			tag := strings.ToLower(node.Data)
			blockTags := map[string]bool{
				"p": true, "h1": true, "h2": true, "h3": true, "h4": true,
				"h5": true, "h6": true, "li": true, "article": true, "main": true, "section": true,
			}
			if blockTags[tag] {
				t := strings.TrimSpace(getNodeText(node, depth+1))
				if len(t) > 3 {
					parts = append(parts, t)
				}
				return
			}
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			walk(c, depth+1)
		}
	}
	walk(n, 0)
	return parts
}
