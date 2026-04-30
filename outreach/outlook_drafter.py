"""Create draft emails in Microsoft Outlook on macOS via AppleScript."""

import html
import subprocess
import sys


def _escape_applescript_string(s: str) -> str:
    """Escape a string for safe embedding in AppleScript."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _html_body(plain_body: str) -> str:
    """Convert a plain-text email body into simple HTML for Outlook."""
    escaped = html.escape(plain_body)
    paragraphs = escaped.split("\n\n")
    html_parts = []
    for p in paragraphs:
        lines = p.split("\n")
        html_parts.append("<p>" + "<br>".join(lines) + "</p>")
    return (
        '<html><body style="font-family: Calibri, Arial, sans-serif; font-size: 14px;">'
        + "".join(html_parts)
        + "</body></html>"
    )


def create_outlook_draft(
    to_email: str,
    subject: str,
    body: str,
    cc: str = None,
) -> dict:
    """Create a draft email in Microsoft Outlook on macOS.

    Args:
        to_email: Recipient email address.
        subject: Email subject line.
        body: Plain-text email body (will be converted to HTML).
        cc: Optional CC email address.

    Returns:
        dict with success status and message.
    """
    if sys.platform != "darwin":
        return {"success": False, "error": "Outlook draft creation is only supported on macOS"}

    html_body = _html_body(body)
    safe_to = _escape_applescript_string(to_email)
    safe_subject = _escape_applescript_string(subject)
    safe_body = _escape_applescript_string(html_body)

    cc_block = ""
    if cc:
        safe_cc = _escape_applescript_string(cc)
        cc_block = f"""
                make new cc recipient at end of cc recipients with properties {{email address:{{address:"{safe_cc}"}}}}"""

    applescript = f"""
tell application "Microsoft Outlook"
    activate
    set newMessage to make new outgoing message with properties {{subject:"{safe_subject}", content:"{safe_body}"}}
    tell newMessage
        make new to recipient at end of to recipients with properties {{email address:{{address:"{safe_to}"}}}}
        {cc_block}
    end tell
    open newMessage
end tell
"""

    try:
        result = subprocess.run(
            ["osascript", "-e", applescript],
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode != 0:
            stderr = result.stderr.strip()
            if "not running" in stderr.lower() or "application isn" in stderr.lower():
                return {
                    "success": False,
                    "error": "Microsoft Outlook is not running. Please open Outlook and try again.",
                }
            return {"success": False, "error": f"AppleScript error: {stderr}"}

        return {"success": True, "message": f"Draft created for {to_email}"}

    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Outlook took too long to respond"}
    except FileNotFoundError:
        return {"success": False, "error": "osascript not found — are you on macOS?"}


def create_batch_drafts(emails: list[dict]) -> list[dict]:
    """Create multiple Outlook drafts.

    Args:
        emails: List of dicts with keys: to_email, subject, body, cc (optional).

    Returns:
        List of result dicts, one per email.
    """
    results = []
    for email_data in emails:
        result = create_outlook_draft(
            to_email=email_data["to_email"],
            subject=email_data["subject"],
            body=email_data["body"],
            cc=email_data.get("cc"),
        )
        results.append(result)
    return results
