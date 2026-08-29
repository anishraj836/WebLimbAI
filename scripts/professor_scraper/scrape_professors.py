#!/usr/bin/env python3
"""
US AI Professor Email Scraper — Powered by AgentLimbs (WebLimbAI)

Scrapes faculty directory pages from top US universities, filters for AI/ML
professors, extracts emails from individual profile pages, and generates
personalized cold-email drafts.

Usage:
    python3 scripts/professor_scraper/scrape_professors.py

Output:
    data/professors_ai.json     — Structured professor data
    data/personalized_emails.md — Ready-to-send personalized emails
"""

import subprocess
import json
import re
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Configuration ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
AGENTLIMBS = str(REPO_ROOT / "agentlimbs")
OUTPUT_DIR = REPO_ROOT / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

# AI/ML related keywords for filtering professors
AI_KEYWORDS = {
    "artificial intelligence", "machine learning", "deep learning",
    "neural network", "natural language processing", "nlp",
    "computer vision", "reinforcement learning", "robotics",
    "data science", "generative model", "large language model", "llm",
    "representation learning", "transfer learning", "federated learning",
    "graph neural", "transformer", "attention mechanism",
    "bayesian", "probabilistic", "optimization",
    "ai", "ml", "cv", "human-centered ai", "responsible ai",
    "fairness", "explainability", "interpretability",
    "signal processing", "speech", "multimodal",
    "autonomous", "perception", "planning",
}

# ── University Faculty Pages ─────────────────────────────────────────────────
# Each entry defines a listing URL and a regex to extract (name, profile_url).
# areas_in_listing=True means the listing page itself shows research areas
# inline, so we can pre-filter before scraping individual profiles.
FACULTY_PAGES = [
    {
        "university": "University of Washington",
        "department": "Paul G. Allen School of CSE",
        "listing_url": "https://www.cs.washington.edu/people/faculty",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cs\.washington\.edu/people/faculty/[^\)]+)\)',
        "areas_in_listing": True,
    },
    {
        "university": "Cornell University",
        "department": "Department of Computer Science",
        "listing_url": "https://www.cs.cornell.edu/people/faculty",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cs\.cornell\.edu/people/[a-z][\w-]+)\)',
        "areas_in_listing": True,
    },
    {
        "university": "Princeton University",
        "department": "Department of Computer Science",
        "listing_url": "https://www.cs.princeton.edu/people/faculty",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cs\.princeton\.edu/people/profile/[^\)]+)\)',
        "areas_in_listing": True,
    },
    {
        "university": "Carnegie Mellon University",
        "department": "Machine Learning Department",
        "listing_url": "https://www.ml.cmu.edu/people/index.html",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https?://[^\)]*cmu[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "University of Illinois Urbana-Champaign",
        "department": "Siebel School of Computing & Data Science",
        "listing_url": "https://siebelschool.illinois.edu/about/people/all-faculty",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://siebelschool\.illinois\.edu/about/people/all-faculty/[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "Georgia Institute of Technology",
        "department": "College of Computing",
        "listing_url": "https://www.cc.gatech.edu/people/faculty",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cc\.gatech\.edu/[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "UC San Diego",
        "department": "CSE Department",
        "listing_url": "https://cse.ucsd.edu/people/faculty",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://cse\.ucsd\.edu/[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "University of Texas at Austin",
        "department": "Department of Computer Science",
        "listing_url": "https://www.cs.utexas.edu/people/faculty-researchers",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cs\.utexas\.edu/[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "University of Maryland",
        "department": "Department of Computer Science",
        "listing_url": "https://www.cs.umd.edu/people/faculty",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cs\.umd\.edu/[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "University of Pennsylvania",
        "department": "Computer and Information Science",
        "listing_url": "https://www.cis.upenn.edu/people/faculty/",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cis\.upenn\.edu/[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "Columbia University",
        "department": "Department of Computer Science",
        "listing_url": "https://www.cs.columbia.edu/directory/",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cs\.columbia\.edu/[^\)]+)\)',
        "areas_in_listing": False,
    },
    {
        "university": "Johns Hopkins University",
        "department": "Department of Computer Science",
        "listing_url": "https://www.cs.jhu.edu/people/faculty/",
        "profile_pattern": r'\[([A-Z][^\]]{2,50})\]\((https://www\.cs\.jhu\.edu/[^\)]+)\)',
        "areas_in_listing": False,
    },
]


def scrape_url(url: str, mode: str = "preserve_links") -> str:
    """Scrape a URL using agentlimbs and return the extracted markdown content."""
    try:
        result = subprocess.run(
            [AGENTLIMBS, "scrape", url, "-j", "--no-index", "-m", mode],
            capture_output=True, text=True, timeout=30,
            cwd=str(REPO_ROOT),
        )
        if result.returncode != 0:
            print(f"  ⚠ Scrape failed for {url}: {result.stdout.strip()}")
            return ""
        data = json.loads(result.stdout)
        return data.get("markdown", "")
    except subprocess.TimeoutExpired:
        print(f"  ⚠ Timeout scraping {url}")
        return ""
    except json.JSONDecodeError:
        print(f"  ⚠ JSON parse error for {url}")
        return ""
    except Exception as e:
        print(f"  ⚠ Error scraping {url}: {e}")
        return ""


def extract_email(text: str) -> str:
    """Extract the first academic email from text."""
    # Match typical academic email patterns
    pattern = r'[\w.+-]+@[\w-]+\.(?:edu|ac\.[a-z]{2}|cs\.\w+\.edu)'
    emails = re.findall(pattern, text, re.IGNORECASE)
    if emails:
        return emails[0]
    # Fallback: any email in the text
    pattern2 = r'[\w.+-]+@[\w.-]+\.\w+'
    emails2 = re.findall(pattern2, text, re.IGNORECASE)
    for e in emails2:
        if any(d in e.lower() for d in ['.edu', 'university', 'cornell', 'stanford',
               'washington', 'princeton', 'illinois', 'gatech', 'ucsd', 'utexas',
               'mit', 'berkeley', 'cmu', 'umich', 'ucla']):
            return e
    return emails2[0] if emails2 else ""


def is_ai_professor(name: str, areas: str, bio: str) -> bool:
    """Check if a professor works in AI/ML based on their research areas and bio."""
    combined = f"{areas} {bio}".lower()
    return any(kw in combined for kw in AI_KEYWORDS)


def extract_research_areas(text: str) -> str:
    """Extract research area strings from scraped text."""
    # Look for common patterns
    areas = []
    for pattern in [
        r'(?:Research\s*Areas?|Expertise|Focus\s*Area|Research\s*Interests?)[:\s]*([^\n]+)',
        r'(?:Areas?|Interests?)[:\s]*([^\n]+)',
    ]:
        matches = re.findall(pattern, text, re.IGNORECASE)
        areas.extend(matches)
    return "; ".join(areas) if areas else ""


def extract_professors_from_listing(page: dict) -> list[dict]:
    """Parse a faculty listing page to extract professor names and profile URLs."""
    print(f"\n{'='*70}")
    print(f"🏛  Scraping: {page['university']} — {page['department']}")
    print(f"   URL: {page['listing_url']}")
    print(f"{'='*70}")

    markdown = scrape_url(page["listing_url"])
    if not markdown:
        print("  ❌ Failed to scrape listing page")
        return []

    professors = []
    pattern = page["profile_pattern"]
    matches = re.findall(pattern, markdown)

    if not matches:
        # Fallback: try generic link + text extraction
        generic = r'\[([A-Z][a-z]+ [A-Z][^\]]{2,40})\]\((https?://[^\)]+)\)'
        matches = re.findall(generic, markdown)

    print(f"  📋 Found {len(matches)} faculty entries on listing page")

    # For pages with areas in the listing, extract inline areas
    for name, profile_url in matches:
        name = name.strip()
        if not name or len(name) < 3:
            continue
        # Skip navigation/menu links
        if any(skip in name.lower() for skip in [
            'menu', 'search', 'home', 'about', 'research', 'academics',
            'contact', 'apply', 'news', 'events', 'view', 'more', 'all ',
            'jump', 'filter', 'sort', 'next', 'previous', 'page'
        ]):
            continue

        prof = {
            "name": name,
            "profile_url": profile_url,
            "university": page["university"],
            "department": page["department"],
            "email": "",
            "title": "",
            "research_areas": "",
            "bio": "",
        }

        # If the listing page includes area info, try to extract it
        if page.get("areas_in_listing"):
            # Find text near this professor's name in the markdown
            idx = markdown.find(name)
            if idx >= 0:
                snippet = markdown[idx:idx+500]
                prof["research_areas"] = extract_research_areas(snippet)
                # Extract title from nearby text
                title_match = re.search(
                    r'(?:Professor|Assistant Professor|Associate Professor|'
                    r'Teaching Professor|Emeritus|Lecturer)[^\n]*',
                    snippet, re.IGNORECASE
                )
                if title_match:
                    prof["title"] = title_match.group(0).strip()

        professors.append(prof)

    return professors


def enrich_professor(prof: dict) -> dict:
    """Scrape an individual professor's profile page for email, bio, and areas."""
    url = prof["profile_url"]
    markdown = scrape_url(url)
    if not markdown:
        return prof

    # Extract email
    email = extract_email(markdown)
    if email:
        prof["email"] = email

    # Extract research areas (if not already set from listing)
    if not prof["research_areas"]:
        prof["research_areas"] = extract_research_areas(markdown)

    # Extract title
    if not prof["title"]:
        title_match = re.search(
            r'(?:###?\s*)?((?:Amazon |Endowed |Named )?'
            r'(?:Assistant |Associate |Full |Research |Teaching |Adjunct )?'
            r'Professor[^\n]*)',
            markdown, re.IGNORECASE
        )
        if title_match:
            prof["title"] = title_match.group(1).strip()

    # Extract bio (first substantial paragraph)
    paragraphs = [p.strip() for p in markdown.split('\n\n') if len(p.strip()) > 100]
    bio_paragraphs = []
    for p in paragraphs:
        p_clean = p.strip()
        if p_clean.startswith('#') or p_clean.startswith('[') or p_clean.startswith('|'):
            continue
        if any(skip in p_clean.lower() for skip in ['cookie', 'privacy', 'menu', 'navigation']):
            continue
        bio_paragraphs.append(p_clean)
        if len(bio_paragraphs) >= 2:
            break
    prof["bio"] = " ".join(bio_paragraphs)

    return prof


def generate_personalized_email(prof: dict, sender_info: dict) -> str:
    """Generate a personalized cold email for a professor."""
    name = prof["name"]
    first_name = name.split()[0] if name else "Professor"
    # Use last name for formal address
    last_name = name.split()[-1] if name else ""
    areas = prof.get("research_areas", "AI/ML")
    university = prof["university"]
    bio = prof.get("bio", "")
    title = prof.get("title", "Professor")

    # Personalize based on research areas
    area_lower = areas.lower() if areas else ""
    bio_lower = bio.lower() if bio else ""

    # Determine specific research hook
    research_hooks = []
    if any(kw in area_lower + bio_lower for kw in ["reinforcement learning", "rl"]):
        research_hooks.append("reinforcement learning and decision-making under uncertainty")
    if any(kw in area_lower + bio_lower for kw in ["natural language", "nlp", "language model", "llm"]):
        research_hooks.append("natural language processing and large language models")
    if any(kw in area_lower + bio_lower for kw in ["computer vision", "vision", "image"]):
        research_hooks.append("computer vision and visual understanding")
    if any(kw in area_lower + bio_lower for kw in ["robotics", "robot"]):
        research_hooks.append("robotics and embodied AI")
    if any(kw in area_lower + bio_lower for kw in ["fairness", "responsible", "ethics", "bias"]):
        research_hooks.append("responsible AI and algorithmic fairness")
    if any(kw in area_lower + bio_lower for kw in ["graph", "network"]):
        research_hooks.append("graph-based learning and network analysis")
    if any(kw in area_lower + bio_lower for kw in ["generative", "diffusion", "gan"]):
        research_hooks.append("generative models and creative AI")
    if any(kw in area_lower + bio_lower for kw in ["optimization", "convex"]):
        research_hooks.append("optimization methods for machine learning")
    if any(kw in area_lower + bio_lower for kw in ["signal processing", "speech", "audio"]):
        research_hooks.append("signal processing and speech/audio intelligence")
    if any(kw in area_lower + bio_lower for kw in ["health", "medical", "biomedical"]):
        research_hooks.append("AI applications in healthcare and biomedical domains")
    if not research_hooks:
        research_hooks.append("artificial intelligence and machine learning")

    hook = research_hooks[0]
    additional_hooks = ", ".join(research_hooks[1:3]) if len(research_hooks) > 1 else ""

    # Build email
    email_text = f"""---
**To:** {prof.get('email', '[email]')}
**Subject:** IIT Kharagpur EE Student — Research Inquiry in {hook.title()}

---

Dear Professor {last_name},

I am {sender_info['name']}, a {sender_info['year']} student in the Department of {sender_info['department']} at the {sender_info['institution']}. I am writing to express my strong interest in your research on **{hook}**{f', as well as {additional_hooks}' if additional_hooks else ''}, and to inquire about potential research opportunities in your group at {university}.

{_generate_bio_specific_paragraph(prof, hook, bio_lower, sender_info)}

During my time at IIT Kharagpur, I have developed a strong foundation in both electrical engineering fundamentals and AI/ML through coursework and self-directed study. {sender_info.get('experience_line', 'I have been actively reading and implementing ideas from recent papers in top venues like NeurIPS, ICML, and CVPR.')} My EE background gives me a unique perspective on the mathematical and systems-level aspects of AI — from signal processing and optimization theory to hardware-aware model design.

I would be grateful for the opportunity to discuss how my background and interests might align with your ongoing or upcoming projects. I have attached my CV for your reference and would be happy to share any additional materials.

Thank you for your time and consideration. I look forward to hearing from you.

Warm regards,
{sender_info['name']}
{sender_info['department']}, {sender_info['institution']}
{sender_info.get('email', '')}
{sender_info.get('website', '')}
"""
    return email_text


def _generate_bio_specific_paragraph(prof, hook, bio_lower, sender_info):
    """Generate a paragraph that references the professor's specific work."""
    name = prof["name"]
    areas = prof.get("research_areas", "")

    # Try to reference specific aspects of their bio
    if "robotics" in bio_lower and "learning" in bio_lower:
        return (f"Your work at the intersection of machine learning and robotics — particularly "
                f"in developing systems that tightly integrate perception, learning, and control — "
                f"deeply resonates with my own interests. As an EE student, I find the challenge of "
                f"bridging theoretical ML advances with real-world robotic systems especially compelling.")
    elif "language model" in bio_lower or "nlp" in bio_lower:
        return (f"I have been closely following recent advances in language modeling and NLP, and "
                f"your group's contributions to this field have been particularly inspiring. "
                f"I am especially interested in how large-scale language models can be made more "
                f"efficient, interpretable, and aligned with human intent.")
    elif "vision" in bio_lower:
        return (f"Your group's research in computer vision and visual understanding has been a "
                f"significant source of inspiration for me. I have been studying foundational "
                f"architectures like Vision Transformers and diffusion models, and I am keen to "
                f"contribute to pushing the boundaries of visual perception and scene understanding.")
    elif "fairness" in bio_lower or "responsible" in bio_lower:
        return (f"I am deeply motivated by the societal implications of AI systems, and your work on "
                f"making AI more fair, accountable, and transparent is a research direction I am "
                f"eager to contribute to. I believe my engineering background can help in designing "
                f"systems that are both technically robust and socially responsible.")
    elif "optimization" in bio_lower:
        return (f"Having studied optimization theory extensively in my EE curriculum, I am fascinated "
                f"by how these mathematical tools underpin modern machine learning. Your research on "
                f"optimization methods resonates strongly with my training, and I am eager to explore "
                f"this intersection further.")
    else:
        return (f"I have been reading several recent papers in {hook}, and your group's contributions "
                f"have stood out for their technical depth and practical impact. I am particularly "
                f"drawn to how your work advances both the theoretical foundations and real-world "
                f"applicability of AI systems.")


def main():
    print("=" * 70)
    print("🔬 AgentLimbs US AI Professor Scraper & Email Personalizer")
    print("=" * 70)
    print(f"Binary: {AGENTLIMBS}")
    print(f"Output: {OUTPUT_DIR}")
    print()

    # ── Sender info (you — the IIT KGP student) ─────────────────────────────
    sender_info = {
        "name": "Anish Raj",
        "year": "undergraduate",
        "department": "Electrical Engineering",
        "institution": "Indian Institute of Technology Kharagpur",
        "email": "[your.email@iitkgp.ac.in]",
        "website": "[your-website-or-linkedin]",
        "experience_line": (
            "I have been actively reading and implementing ideas from recent "
            "papers published at NeurIPS, ICML, CVPR, and ACL, and I have "
            "hands-on experience with deep learning frameworks (PyTorch) and "
            "building ML pipelines."
        ),
    }

    all_professors = []

    # ── Phase 1: Scrape listing pages ────────────────────────────────────────
    for page in FACULTY_PAGES:
        professors = extract_professors_from_listing(page)
        all_professors.extend(professors)
        time.sleep(1)  # Be respectful to servers

    print(f"\n📊 Total professors found across all universities: {len(all_professors)}")

    # ── Phase 2: Enrich with profile page scrapes (get emails & bios) ───────
    # Only scrape profiles for professors that look AI-related from listing data
    # For pages without areas in listing, we scrape ALL profiles first
    to_enrich = []
    already_ai = []

    for prof in all_professors:
        if prof["research_areas"] and is_ai_professor(prof["name"], prof["research_areas"], prof["bio"]):
            already_ai.append(prof)
            to_enrich.append(prof)  # Still need email
        elif not prof["research_areas"]:
            to_enrich.append(prof)  # Need to check profile for areas

    print(f"\n🔍 Enriching {len(to_enrich)} professor profiles for email + research areas...")
    print(f"   (Already identified {len(already_ai)} as AI-related from listing)")

    # Rate-limited sequential scraping to be respectful
    enriched = []
    for i, prof in enumerate(to_enrich):
        if i > 0 and i % 10 == 0:
            print(f"   Progress: {i}/{len(to_enrich)} profiles scraped...")
            time.sleep(2)  # Pause every 10 requests

        prof = enrich_professor(prof)
        enriched.append(prof)
        time.sleep(0.5)  # 0.5s between requests

    # ── Phase 3: Filter for AI professors ────────────────────────────────────
    ai_professors = []
    for prof in enriched:
        if is_ai_professor(prof["name"], prof["research_areas"], prof["bio"]):
            if prof["email"]:  # Only include professors with emails
                ai_professors.append(prof)

    print(f"\n✅ Found {len(ai_professors)} AI/ML professors with email addresses")

    # ── Phase 4: Save structured data ────────────────────────────────────────
    output_file = OUTPUT_DIR / "professors_ai.json"
    with open(output_file, "w") as f:
        json.dump(ai_professors, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Professor data saved to: {output_file}")

    # ── Phase 5: Generate personalized emails ────────────────────────────────
    email_file = OUTPUT_DIR / "personalized_emails.md"
    with open(email_file, "w") as f:
        f.write("# Personalized Cold Emails for US AI Professors\n\n")
        f.write(f"*Generated on {time.strftime('%Y-%m-%d %H:%M')} using AgentLimbs*\n\n")
        f.write(f"**Total professors:** {len(ai_professors)}\n\n")
        f.write("---\n\n")

        for i, prof in enumerate(ai_professors, 1):
            f.write(f"## {i}. {prof['name']} — {prof['university']}\n\n")
            f.write(f"- **Title:** {prof.get('title', 'N/A')}\n")
            f.write(f"- **Email:** {prof.get('email', 'N/A')}\n")
            f.write(f"- **Research Areas:** {prof.get('research_areas', 'N/A')}\n")
            f.write(f"- **Profile:** {prof.get('profile_url', 'N/A')}\n\n")
            email_text = generate_personalized_email(prof, sender_info)
            f.write(email_text)
            f.write("\n\n---\n\n")

    print(f"📧 Personalized emails saved to: {email_file}")

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    by_university = {}
    for prof in ai_professors:
        uni = prof["university"]
        by_university.setdefault(uni, []).append(prof)

    for uni, profs in sorted(by_university.items()):
        print(f"  🏛  {uni}: {len(profs)} AI professors")
        for p in profs[:3]:
            print(f"      • {p['name']} ({p['email']})")
        if len(profs) > 3:
            print(f"      ... and {len(profs)-3} more")
    print(f"\n  Total: {len(ai_professors)} professors across {len(by_university)} universities")
    print("=" * 70)


if __name__ == "__main__":
    main()
