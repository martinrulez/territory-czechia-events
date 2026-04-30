"""Static HTML scraper using requests + BeautifulSoup."""

import re
import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,cs;q=0.8",
}


def fetch_page(url: str, timeout: int = 30) -> BeautifulSoup:
    """Fetch a URL and return parsed BeautifulSoup."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def _clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _extract_from_img_alt(soup: BeautifulSoup, selector: str) -> list[str]:
    """Extract company names from image alt attributes."""
    names = []
    for img in soup.select(selector):
        alt = img.get("alt", "").strip()
        if alt and len(alt) > 1 and alt.lower() not in ("logo", "image", "partner", "sponsor"):
            alt = re.sub(r"\s*(logo|Logo|LOGO)\s*", "", alt).strip()
            if alt:
                names.append(alt)
    return names


def scrape_game_access(url: str) -> list[dict]:
    """Scrape Game Access Conference speakers and partners."""
    results = []
    try:
        soup = fetch_page(url)

        for section in soup.find_all(["div", "section"]):
            heading = section.find(["h2", "h3"])
            if not heading:
                continue
            heading_text = heading.get_text(strip=True).lower()

            if "speaker" in heading_text:
                for card in section.find_all("div", recursive=True):
                    name_el = card.find("h3") or card.find("h4")
                    if not name_el:
                        continue
                    name = _clean_text(name_el.get_text())
                    if not name or len(name) < 3:
                        continue
                    paragraphs = card.find_all("p")
                    title = _clean_text(paragraphs[0].get_text()) if paragraphs else ""
                    results.append({
                        "company_name": title.split(",")[-1].strip() if "," in title else "",
                        "role": "speaker_company",
                        "person_name": name,
                        "person_title": title,
                    })

            if "partner" in heading_text or "sponsor" in heading_text:
                for img in section.find_all("img"):
                    alt = img.get("alt", "").strip()
                    if alt and len(alt) > 1:
                        alt = re.sub(r"\s*(logo|Logo)\s*", "", alt).strip()
                        if alt:
                            results.append({"company_name": alt, "role": "partner"})

    except Exception:
        pass
    return results


def scrape_3dise(url: str) -> list[dict]:
    """Scrape 3DISE Conference speakers, exhibitors, and partners."""
    results = []
    try:
        soup = fetch_page(url)

        for section in soup.find_all(["div", "section"]):
            heading = section.find(["h2", "h3"])
            if not heading:
                continue
            heading_text = heading.get_text(strip=True).lower()

            if "speaker" in heading_text or "panelist" in heading_text:
                for card in section.find_all("div", recursive=True):
                    h3 = card.find("h3")
                    if not h3:
                        continue
                    name = _clean_text(h3.get_text())
                    if not name or len(name) < 3:
                        continue
                    linkedin_a = card.find("a", href=re.compile(r"linkedin", re.I))
                    linkedin = linkedin_a["href"] if linkedin_a else ""
                    paragraphs = card.find_all("p")
                    title = ""
                    for p in paragraphs:
                        p_text = _clean_text(p.get_text())
                        if p_text and p_text != name:
                            title = p_text
                            break
                    results.append({
                        "company_name": title.split(" at ")[-1].strip() if " at " in title else "",
                        "role": "speaker_company",
                        "person_name": name,
                        "person_title": title,
                        "person_linkedin": linkedin,
                    })

            if "exhibitor" in heading_text:
                for img in section.find_all("img"):
                    alt = img.get("alt", "").strip()
                    if alt and len(alt) > 1:
                        results.append({"company_name": alt, "role": "exhibitor"})

            if "partner" in heading_text:
                for img in section.find_all("img"):
                    alt = img.get("alt", "").strip()
                    if alt and len(alt) > 1:
                        results.append({"company_name": alt, "role": "partner"})

    except Exception:
        pass
    return results


def scrape_connected_construction(url: str) -> list[dict]:
    """Scrape Connected Construction Days partners/sponsors."""
    results = []
    try:
        soup = fetch_page(url)
        for img in soup.find_all("img"):
            alt = img.get("alt", "").strip()
            if alt and len(alt) > 2:
                alt_clean = re.sub(r"\s*(logo|Logo|_logo)\s*", "", alt).strip()
                if alt_clean and alt_clean.lower() not in ("connected", "construction", "days", "ccd"):
                    results.append({"company_name": alt_clean, "role": "partner"})
    except Exception:
        pass
    return results


def scrape_lean_summit(url: str) -> list[dict]:
    """Scrape Lean Summit companies from gemba walks and sponsors."""
    results = []
    try:
        soup = fetch_page(url)
        for img in soup.find_all("img"):
            alt = img.get("alt", "").strip()
            src = img.get("src", "")
            if alt and len(alt) > 2 and ("sponsor" in src.lower() or "partner" in src.lower() or "gemba" in src.lower()):
                results.append({"company_name": alt, "role": "partner"})
    except Exception:
        pass
    return results


def scrape_digital_factory(url: str) -> list[dict]:
    """Scrape Digital Factory / MSV exhibitors and speakers."""
    results = []
    try:
        soup = fetch_page(url)
        for img in soup.find_all("img"):
            alt = img.get("alt", "").strip()
            if alt and len(alt) > 2:
                alt_clean = re.sub(r"\s*(logo|Logo)\s*", "", alt).strip()
                if alt_clean:
                    results.append({"company_name": alt_clean, "role": "exhibitor"})
    except Exception:
        pass
    return results


def scrape_generic_exhibitor_page(url: str, role: str = "exhibitor") -> list[dict]:
    """Generic scraper for exhibitor list pages. Extracts company names from
    links, headings, and image alt tags in common exhibitor directory layouts.
    """
    results = []
    try:
        soup = fetch_page(url)
    except Exception:
        return results

    seen = set()

    for img in soup.select("img[alt]"):
        alt = _clean_text(img.get("alt", ""))
        if alt and len(alt) > 2 and len(alt) < 80 and alt.lower() not in seen:
            skip = any(kw in alt.lower() for kw in (
                "logo", "banner", "icon", "arrow", "button", "facebook",
                "twitter", "linkedin", "instagram", "youtube", "menu",
                "header", "footer", "close", "search", "http", "www.",
                "placeholder", "loading", ".jpg", ".png", ".svg",
            ))
            if not skip:
                seen.add(alt.lower())
                results.append({"company_name": alt, "role": role})

    for link in soup.select("a[href*='exhibitor'], a[href*='vystavovatel'], .exhibitor-name, .exhibitor a"):
        name = _clean_text(link.get_text())
        if name and len(name) > 2 and len(name) < 80 and name.lower() not in seen:
            seen.add(name.lower())
            href = link.get("href", "")
            domain = None
            if href and "http" in href and "exhibitor" not in href:
                domain = href.split("//")[-1].split("/")[0] if "//" in href else None
            results.append({"company_name": name, "role": role, "company_domain": domain})

    for heading in soup.select("h3, h4, h5"):
        parent_classes = " ".join(heading.parent.get("class", []) if heading.parent else []).lower()
        if any(kw in parent_classes for kw in ("exhibitor", "partner", "sponsor", "company")):
            name = _clean_text(heading.get_text())
            if name and len(name) > 2 and len(name) < 80 and name.lower() not in seen:
                seen.add(name.lower())
                results.append({"company_name": name, "role": role})

    return results


def scrape_for_arch(url: str) -> list[dict]:
    """Scrape FOR ARCH construction fair exhibitors."""
    results = []
    for page_url in [url, url.rstrip("/") + "/vystavovatele"]:
        try:
            results.extend(scrape_generic_exhibitor_page(page_url, role="exhibitor"))
        except Exception:
            continue
    return results


def scrape_urbis(url: str) -> list[dict]:
    """Scrape URBIS Smart Cities exhibitors and partners."""
    results = []
    for page_url in [
        url,
        "https://www.smartcityfair.cz/en/urbis/exhibitor-list",
        "https://www.smartcityfair.cz/en/",
    ]:
        try:
            results.extend(scrape_generic_exhibitor_page(page_url, role="exhibitor"))
        except Exception:
            continue
    return results


def scrape_architect_at_work(url: str) -> list[dict]:
    """Scrape Architect@Work Prague exhibitors."""
    results = []
    for page_url in [
        url,
        "https://www.architectatwork.com/en/events/a@w-prague/exhibitors",
    ]:
        try:
            results.extend(scrape_generic_exhibitor_page(page_url, role="exhibitor"))
        except Exception:
            continue
    return results


SITE_SCRAPERS = {
    "game_access": lambda cfg: scrape_game_access(cfg["url"]),
    "3dise": lambda cfg: scrape_3dise(cfg["url"]),
    "connected_construction": lambda cfg: scrape_connected_construction(cfg["url"]),
    "lean_summit": lambda cfg: scrape_lean_summit(cfg["url"]),
    "digital_factory": lambda cfg: scrape_digital_factory(cfg["url"]),
    "for_arch": lambda cfg: scrape_for_arch(cfg["url"]),
    "urbis_smart_cities": lambda cfg: scrape_urbis(cfg["url"]),
    "architect_at_work": lambda cfg: scrape_architect_at_work(cfg["url"]),
}
