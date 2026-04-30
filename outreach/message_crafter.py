"""LLM-powered message crafting for personalized sales outreach.

Supports two modes:
- API mode: calls OpenAI/Anthropic directly (requires API key in .env)
- Manual mode: builds the full prompt so you can paste it into Cursor or any AI chat
"""

import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
PLAYS_DIR = PROMPTS_DIR / "plays"


def _has_api_key() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


def _load_system_prompt() -> str:
    path = PROMPTS_DIR / "system_prompt.md"
    with open(path, "r") as f:
        return f.read()


def _load_play(play_name: str) -> str:
    path = PLAYS_DIR / f"{play_name}.md"
    if not path.exists():
        return ""
    with open(path, "r") as f:
        return f.read()


def list_plays() -> list[str]:
    """Return available play names."""
    return [p.stem for p in PLAYS_DIR.glob("*.md")]


def build_prospect_context(
    contact: dict = None,
    account: dict = None,
    opportunity: dict = None,
    enrichment: dict = None,
    custom_notes: str = None,
) -> str:
    """Build a context block from all available prospect data."""
    sections = []

    if account:
        lines = ["## Account"]
        for key in ("company_name", "domain", "industry", "employee_count", "autodesk_products", "account_status"):
            val = account.get(key)
            if val:
                lines.append(f"- {key.replace('_', ' ').title()}: {val}")
        sections.append("\n".join(lines))

    if contact:
        lines = ["## Contact"]
        for key in ("first_name", "last_name", "title", "email", "phone", "linkedin_url"):
            val = contact.get(key)
            if val:
                lines.append(f"- {key.replace('_', ' ').title()}: {val}")
        sections.append("\n".join(lines))

    if opportunity:
        lines = ["## Opportunity"]
        for key in ("opp_name", "stage", "products", "value", "close_date"):
            val = opportunity.get(key)
            if val:
                lines.append(f"- {key.replace('_', ' ').title()}: {val}")
        sections.append("\n".join(lines))

    if enrichment:
        lines = ["## Enrichment Data"]
        skip_keys = {"success", "from_cache", "error"}
        for key, val in enrichment.items():
            if key in skip_keys or not val:
                continue
            if isinstance(val, list):
                val = ", ".join(str(v) for v in val[:10])
            lines.append(f"- {key.replace('_', ' ').title()}: {val}")
        sections.append("\n".join(lines))

    if custom_notes:
        sections.append(f"## Additional Notes\n{custom_notes}")

    return "\n\n".join(sections) if sections else "No prospect data available."


def _build_full_prompt(play_name: str, prospect_context: str) -> str:
    """Build the complete prompt ready for copy-paste into Cursor."""
    system_prompt = _load_system_prompt()
    play_prompt = _load_play(play_name)

    return f"""{system_prompt}

---

## Play Instructions
{play_prompt}

## Prospect Context
{prospect_context}

Generate the outreach email now. Return ONLY a JSON object with "subject" and "body" keys."""


def _build_regeneration_prompt(
    previous_subject: str,
    previous_body: str,
    feedback: str,
    play_name: str,
    prospect_context: str,
) -> str:
    """Build a regeneration prompt ready for copy-paste."""
    system_prompt = _load_system_prompt()
    play_prompt = _load_play(play_name)

    return f"""{system_prompt}

---

## Play Instructions
{play_prompt}

## Prospect Context
{prospect_context}

## Previous Draft
Subject: {previous_subject}
Body:
{previous_body}

## Feedback
{feedback}

Revise the email based on the feedback. Return ONLY a JSON object with "subject" and "body" keys."""


def _call_openai(prompt: str) -> dict:
    """Call OpenAI API with the given prompt."""
    from openai import OpenAI

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "user", "content": prompt},
        ],
        max_tokens=1024,
        temperature=0.7,
    )

    raw = response.choices[0].message.content.strip()
    return _parse_json_response(raw)


def _parse_json_response(raw: str) -> dict:
    """Parse a JSON response, stripping markdown fences if present."""
    if raw.startswith("```"):
        lines = raw.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines)

    try:
        result = json.loads(raw)
        result["success"] = True
        return result
    except json.JSONDecodeError:
        return {"success": False, "error": "Failed to parse response as JSON. Make sure you paste only the JSON object.", "raw": raw}


def craft_message(
    play_name: str,
    contact: dict = None,
    account: dict = None,
    opportunity: dict = None,
    enrichment: dict = None,
    custom_notes: str = None,
) -> dict:
    """Generate a personalized outreach email for a prospect.

    If an API key is configured, calls the LLM directly.
    Otherwise, returns the prompt for manual use.
    """
    prospect_context = build_prospect_context(contact, account, opportunity, enrichment, custom_notes)
    prompt = _build_full_prompt(play_name, prospect_context)

    if _has_api_key():
        return _call_openai(prompt)

    return {"success": False, "mode": "manual", "prompt": prompt}


def regenerate_with_feedback(
    previous_subject: str,
    previous_body: str,
    feedback: str,
    play_name: str,
    contact: dict = None,
    account: dict = None,
    opportunity: dict = None,
    enrichment: dict = None,
) -> dict:
    """Regenerate an email incorporating user feedback."""
    prospect_context = build_prospect_context(contact, account, opportunity, enrichment)
    prompt = _build_regeneration_prompt(previous_subject, previous_body, feedback, play_name, prospect_context)

    if _has_api_key():
        return _call_openai(prompt)

    return {"success": False, "mode": "manual", "prompt": prompt}


def parse_ai_response(response_text: str) -> dict:
    """Parse the AI's response text (pasted back by the user) into subject/body."""
    return _parse_json_response(response_text.strip())
