package extractor

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestConvertHTMLToMarkdown(t *testing.T) {
	htmlInput := `<!DOCTYPE html>
<html>
<head>
    <title>Test Page</title>
    <script>console.log("ignore me");</script>
    <style>body { color: red; }</style>
</head>
<body>
    <header><h1>Nav Header</h1></header>
    <nav><a href="/home">Home</a></nav>
    <main>
        <h1>Main Title</h1>
        <p>This is a paragraph with a <a href="https://example.com/link">link</a> and <code>inline code</code>.</p>
        <h2>Section 2</h2>
        <ul>
            <li>First item</li>
            <li>Second item</li>
        </ul>
        <pre>func main() {}</pre>
        <iframe>http://example.com/frame</iframe>
    </main>
    <footer>Copyright 2026</footer>
</body>
</html>`

	md, tokens, title := ConvertHTMLToMarkdown("https://example.com", []byte(htmlInput), "clean_rag")

	if title != "Test Page" {
		t.Errorf("Expected title 'Test Page', got '%s'", title)
	}

	if tokens <= 0 {
		t.Errorf("Expected positive token count, got %d", tokens)
	}

	// Verify ignored non-content nodes
	if strings.Contains(md, "console.log") {
		t.Errorf("Expected script tag to be ignored")
	}
	if strings.Contains(md, "color: red") {
		t.Errorf("Expected style tag to be ignored")
	}
	if strings.Contains(md, "Nav Header") {
		t.Errorf("Expected header tag to be ignored")
	}
	if strings.Contains(md, "Copyright 2026") {
		t.Errorf("Expected footer tag to be ignored")
	}
	if strings.Contains(md, "http://example.com/frame") {
		t.Errorf("Expected iframe tag to be ignored")
	}

	// Verify converted elements
	if !strings.Contains(md, "# Main Title") {
		t.Errorf("Expected '# Main Title' in markdown")
	}
	if !strings.Contains(md, "## Section 2") {
		t.Errorf("Expected '## Section 2' in markdown")
	}
	if !strings.Contains(md, "[link](https://example.com/link)") {
		t.Errorf("Expected link markdown in output, got:\n%s", md)
	}
	if !strings.Contains(md, "`inline code`") {
		t.Errorf("Expected inline code in markdown")
	}
	if !strings.Contains(md, "- First item") {
		t.Errorf("Expected '- First item' list element")
	}
	if !strings.Contains(md, "```\nfunc main() {}\n```") {
		t.Errorf("Expected pre code block in markdown")
	}
}

func TestProcessRawHTML(t *testing.T) {
	htmlInput := `<html lang="en">
<head>
    <title>Doc Title</title>
    <meta name="pubdate" content="2026-08-08">
</head>
<body>
    <p>Sample body text here.</p>
    <a href="/page2">Link to Page 2</a>
</body>
</html>`

	doc, err := ProcessRawHTML("https://example.com/base", []byte(htmlInput))
	if err != nil {
		t.Fatalf("ProcessRawHTML failed: %v", err)
	}

	if doc.Title != "Doc Title" {
		t.Errorf("Expected title 'Doc Title', got '%s'", doc.Title)
	}
	if doc.Language != "en" {
		t.Errorf("Expected lang 'en', got '%s'", doc.Language)
	}
	if doc.Timestamp != "2026-08-08" {
		t.Errorf("Expected pubdate '2026-08-08', got '%s'", doc.Timestamp)
	}
	if len(doc.Links) != 1 || doc.Links[0] != "https://example.com/page2" {
		t.Errorf("Expected resolved link 'https://example.com/page2', got %v", doc.Links)
	}
	if !strings.Contains(doc.Body, "Sample body text here.") {
		t.Errorf("Expected body content in CleanDocument, got '%s'", doc.Body)
	}
}

func TestExtractFields(t *testing.T) {
	mdText := "# Company Name\nACME Corp\n\n- Revenue: $1,000,000"
	extracted := ExtractFields(mdText, []string{"Revenue", "Company"})

	if extracted["Revenue"] != "Revenue: $1,000,000" {
		t.Errorf("Expected Revenue field extraction, got '%s'", extracted["Revenue"])
	}
}

func TestCountBPETokens_SpecialTokens(t *testing.T) {
	specialTexts := []string{
		"Hello <|endoftext|> world",
		"<|im_start|>system\nYou are an AI assistant.<|im_end|>",
		"Code with <|fim_prefix|> prefix <|fim_suffix|> suffix <|fim_middle|> middle",
		"",
	}

	for _, text := range specialTexts {
		// Should count tokens without panicking
		tokens := CountBPETokens(text)
		if text != "" && tokens <= 0 {
			t.Errorf("Expected positive token count for %q, got %d", text, tokens)
		}
	}
}

func TestConvertHTMLToMarkdown_NestedTables(t *testing.T) {
	htmlInput := `<!DOCTYPE html>
<html>
<head><title>Nested Table Page</title></head>
<body>
    <table id="outer">
        <thead>
            <tr><th>OuterCol1</th><th>OuterCol2</th></tr>
        </thead>
        <tbody>
            <tr>
                <td>OuterVal1</td>
                <td>
                    <table id="inner">
                        <tr><th>InnerColA</th><th>InnerColB</th></tr>
                        <tr><td>InnerValA</td><td>InnerValB</td></tr>
                    </table>
                </td>
            </tr>
            <tr>
                <td>OuterVal2</td>
                <td>OuterVal3</td>
            </tr>
        </tbody>
    </table>
</body>
</html>`

	md, _, _ := ConvertHTMLToMarkdown("https://example.com", []byte(htmlInput), "clean_rag")

	if !strings.Contains(md, "| OuterCol1 | OuterCol2 |") {
		t.Errorf("Expected outer table header row '| OuterCol1 | OuterCol2 |', got:\n%s", md)
	}

	if !strings.Contains(md, "| InnerColA | InnerColB |") {
		t.Errorf("Expected inner table header row '| InnerColA | InnerColB |', got:\n%s", md)
	}

	if strings.Contains(md, "| OuterCol1 | OuterCol2 | InnerColA |") {
		t.Errorf("Outer table headers corrupted with inner headers:\n%s", md)
	}
}

func TestTokenReduction_DocumentationPage(t *testing.T) {
	// Build a realistic documentation webpage containing standard web boilerplate:
	// - Top header with navigation links and search bar
	// - Left sidebar with 35 table-of-contents links
	// - Large inline script blocks (analytics, telemetry, hydration bundles)
	// - Inline CSS stylesheets
	// - Footer with legal disclaimers, sitemap, and copyright
	// - Core content: technical tutorial with code block and explanations

	var sb strings.Builder
	sb.WriteString(`<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8">
<title>Building Distributed Systems in Go - Documentation</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background: #fafafa; }
header { background: #fff; border-bottom: 1px solid #eaeaea; height: 64px; display: flex; align-items: center; justify-content: space-between; padding: 0 24px; }
.nav-link { color: #555; text-decoration: none; margin-right: 16px; font-size: 14px; }
.sidebar { width: 280px; border-right: 1px solid #eaeaea; padding: 20px; float: left; height: 100vh; overflow-y: auto; }
.content { margin-left: 320px; padding: 40px; max-width: 800px; line-height: 1.6; }
footer { border-top: 1px solid #eaeaea; padding: 32px 24px; background: #fff; margin-top: 80px; color: #888; font-size: 13px; }
</style>
<script>
(function(window, document, tag, url, name) {
    window['AnalyticsObject'] = name;
    window[name] = window[name] || function() { (window[name].q = window[name].q || []).push(arguments); };
    window[name].l = 1 * new Date();
    var script = document.createElement(tag), first = document.getElementsByTagName(tag)[0];
    script.async = 1; script.src = url; first.parentNode.insertBefore(script, first);
})(window, document, 'script', 'https://analytics.example.com/bundle.v2.3.1.js', 'ga');
ga('create', 'UA-98765432-1', 'auto');
ga('send', 'pageview');
console.log("Telemetry initialized for doc page v1.5");
</script>
</head>
<body>
<header>
    <div class="logo"><a href="/"><strong>CloudDocs API</strong></a></div>
    <nav>
        <a class="nav-link" href="/getting-started">Getting Started</a>
        <a class="nav-link" href="/guides">Guides</a>
        <a class="nav-link" href="/api-reference">API Reference</a>
        <a class="nav-link" href="/pricing">Pricing</a>
        <a class="nav-link" href="/changelog">Changelog</a>
        <a class="nav-link" href="/community">Community Forum</a>
        <a class="nav-link" href="/support">Help & Support</a>
        <a class="nav-link" href="/login">Sign In</a>
    </nav>
</header>
<aside class="sidebar">
    <nav>
        <h3>Table of Contents</h3>
        <ul>`)

	for i := 1; i <= 35; i++ {
		sb.WriteString(fmt.Sprintf(`<li><a href="/guide/section-%d">Section %d: Distributed Architecture and Protocols Overview</a></li>`, i, i))
	}

	sb.WriteString(`</ul>
    </nav>
</aside>
<main class="content">
    <article>
        <h1>Distributed Consensus in Go</h1>
        <p>In distributed computing, consensus algorithms ensure that multiple nodes agree on a shared state machine log even in the presence of network partitions and node crashes.</p>
        <p>The Raft consensus protocol achieves this by electing a single leader responsible for log replication. If the leader fails, followers start a new election term using randomized election timeouts.</p>
        <h2>Configuring Raft Nodes</h2>
        <p>Here is an example demonstrating node configuration using Go:</p>
        <pre><code>func NewRaftCluster(nodeID string, peers []string) *Raft {
    return &Raft{
        id: nodeID,
        peers: peers,
        state: Follower,
        heartbeatTimeout: 150 * time.Millisecond,
    }
}</code></pre>
        <p>Ensure that all peer addresses are reachable via TCP before initializing elections.</p>
    </article>
</main>
<footer>
    <div class="footer-links">
        <a href="/privacy">Privacy Policy</a> | 
        <a href="/terms">Terms of Service</a> | 
        <a href="/security">Security Disclosures</a> | 
        <a href="/compliance">SOC2 Compliance</a> | 
        <a href="/status">System Status</a>
    </div>
    <p>&copy; 2026 CloudDocs Platform Inc. All rights reserved. Various trademarks held by their respective owners.</p>
</footer>
</body></html>`)

	rawHTML := sb.String()
	rawTokens := CountBPETokens(rawHTML)

	cleanMarkdown, cleanTokens, title := ConvertHTMLToMarkdown("https://docs.example.com/raft", []byte(rawHTML), "clean_rag")

	if title == "" {
		t.Errorf("Expected title, got empty")
	}

	reductionPct := float64(rawTokens-cleanTokens) / float64(rawTokens) * 100.0

	t.Logf("=== Token Reduction Benchmark Results ===")
	t.Logf("Raw HTML Bytes       : %d bytes", len(rawHTML))
	t.Logf("Raw HTML BPE Tokens   : %d tokens", rawTokens)
	t.Logf("Clean Markdown Bytes  : %d bytes", len(cleanMarkdown))
	t.Logf("Clean Markdown Tokens : %d tokens", cleanTokens)
	t.Logf("Token Reduction       : %.2f%%", reductionPct)
	t.Logf("\n--- EXACT CLEAN MARKDOWN EXTRACTED ---\n%s\n--------------------------------------", cleanMarkdown)

	if reductionPct < 80.0 {
		t.Errorf("Expected token reduction >= 80%%, got %.2f%%", reductionPct)
	}

	// Verify that boilerplate was eliminated
	if strings.Contains(cleanMarkdown, "Telemetry initialized") {
		t.Errorf("Script content leaked into clean markdown")
	}
	if strings.Contains(cleanMarkdown, "font-family") {
		t.Errorf("Style content leaked into clean markdown")
	}
	if strings.Contains(cleanMarkdown, "Table of Contents") {
		t.Errorf("Sidebar navigation leaked into clean markdown")
	}
	if strings.Contains(cleanMarkdown, "Privacy Policy") {
		t.Errorf("Footer boilerplate leaked into clean markdown")
	}

	// Verify core article survived
	if !strings.Contains(cleanMarkdown, "Distributed Consensus in Go") {
		t.Errorf("Article heading missing from markdown")
	}
	if !strings.Contains(cleanMarkdown, "NewRaftCluster") {
		t.Errorf("Code block missing from markdown")
	}
}

func TestTokenReduction_LiveGoDocs(t *testing.T) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://go.dev/doc/tutorial/getting-started")
	if err != nil {
		t.Skipf("Skipping live test due to network unavailability: %v", err)
		return
	}
	defer resp.Body.Close()

	rawHTMLBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	rawTokens := CountBPETokens(string(rawHTMLBytes))
	cleanMD, cleanTokens, title := ConvertHTMLToMarkdown("https://go.dev/doc/tutorial/getting-started", rawHTMLBytes, "clean_rag")

	reductionPct := float64(rawTokens-cleanTokens) / float64(rawTokens) * 100.0

	t.Logf("================ LIVE GO DOCS BENCHMARK ================")
	t.Logf("URL                  : https://go.dev/doc/tutorial/getting-started")
	t.Logf("Title                : %s", title)
	t.Logf("Raw HTML Size        : %d bytes (%d tokens)", len(rawHTMLBytes), rawTokens)
	t.Logf("Clean Markdown Size  : %d bytes (%d tokens)", len(cleanMD), cleanTokens)
	t.Logf("Token Reduction      : %.2f%%", reductionPct)
	t.Logf("Tokens Saved for LLM : %d tokens eliminated", rawTokens-cleanTokens)
	preview := cleanMD
	if len(preview) > 600 {
		preview = preview[:600] + "\n... [truncated for preview]"
	}
	t.Logf("\n--- LIVE EXTRACTED MARKDOWN PREVIEW ---\n%s\n---------------------------------------", preview)
	t.Logf("========================================================")
}

