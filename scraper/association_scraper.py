"""Czech industry association member directory scrapers.

Scrapes member lists from:
- SPS (Svaz podnikatelu ve stavebnictvi) -- Czech construction association
- SPCR (Svaz prumyslu a dopravy CR) -- Czech industry & transport confederation
- Svaz Slevaren (Czech Foundry Association) -- D&M relevant
- CZGBC (Czech Green Building Council) -- AEC relevant
- Moravian Aviation Cluster -- D&M/aerospace relevant
- HK CR Brno (Regional Chamber of Commerce) -- general industry
"""

import re

from scraper.static_scraper import fetch_page

SPS_URL = "https://www.sps.cz/o-sps/seznam-clenu/"
SPS_FIRMY_URL = "https://firmy.sps.cz/"

SPCR_MEMBERS_URL = "https://www.spcr.cz/en/membership/membership-base"

SLEVAREN_URL = "https://www.svazslevaren.cz/slevarny"

CZGBC_URL = "http://czgbc.org/en/members/list"

MORAVIAN_AVIATION_URL = "https://www.czech-aerospace.cz/f-nasi-clenove"

HKCR_BRNO_URL = "https://ohkbv.cz/clenstvi/seznam-clenu/"


def scrape_sps_members() -> list[dict]:
    """Scrape SPS (construction association) member directory."""
    results = []

    try:
        soup = fetch_page(SPS_URL)

        for a_tag in soup.find_all("a"):
            text = a_tag.get_text(strip=True)
            if not text or len(text) < 3:
                continue
            href = a_tag.get("href", "")
            if "sps.cz" in href or "firmy" in href:
                continue
            parent = a_tag.parent
            if parent and parent.name in ("li", "td", "p"):
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 3 and not text.startswith("http"):
                    results.append({
                        "company_name": text,
                        "role": "association_member",
                        "company_domain": "",
                    })

        for li in soup.find_all("li"):
            text = li.get_text(strip=True)
            if not text or len(text) < 4 or len(text) > 100:
                continue
            if li.find("ul") or li.find("ol"):
                continue
            text = re.sub(r"\s+", " ", text).strip()
            if any(c.isupper() for c in text[:3]):
                results.append({
                    "company_name": text,
                    "role": "association_member",
                    "company_domain": "",
                })

    except Exception:
        pass

    try:
        soup2 = fetch_page(SPS_FIRMY_URL)
        for tag in soup2.find_all(["h2", "h3", "h4", "strong"]):
            text = tag.get_text(strip=True)
            if text and len(text) > 3 and len(text) < 100:
                text = re.sub(r"\s+", " ", text).strip()
                results.append({
                    "company_name": text,
                    "role": "association_member",
                    "company_domain": "",
                })
    except Exception:
        pass

    seen = set()
    deduped = []
    for c in results:
        key = c["company_name"].lower().strip()
        skip_words = ("menu", "home", "kontakt", "o sps", "prihlasit",
                      "registrace", "search", "hledat", "cookie", "gdpr",
                      "navigace", "copyright", "footer", "header")
        if key in seen or any(w in key for w in skip_words):
            continue
        seen.add(key)
        deduped.append(c)

    return deduped


def scrape_spcr_members() -> list[dict]:
    """Scrape SPCR (industry confederation) member associations and companies."""
    results = []

    try:
        soup = fetch_page(SPCR_MEMBERS_URL)

        for a_tag in soup.find_all("a"):
            text = a_tag.get_text(strip=True)
            if not text or len(text) < 4:
                continue
            href = a_tag.get("href", "")
            if "/membership/" in href or "/seznam-clenu/" in href:
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) > 4 and len(text) < 120:
                    results.append({
                        "company_name": text,
                        "role": "association_member",
                        "company_domain": "",
                    })

        for li in soup.find_all("li"):
            text = li.get_text(strip=True)
            if text and len(text) > 4 and len(text) < 120:
                links_in = li.find_all("a")
                if links_in:
                    for link in links_in:
                        link_text = link.get_text(strip=True)
                        if link_text and len(link_text) > 4:
                            results.append({
                                "company_name": link_text,
                                "role": "association_member",
                                "company_domain": "",
                            })

    except Exception:
        pass

    seen = set()
    deduped = []
    for c in results:
        key = c["company_name"].lower().strip()
        skip_words = ("menu", "home", "contact", "login", "search",
                      "cookie", "gdpr", "footer", "header", "membership",
                      "about", "news", "event", "publications")
        if key in seen or any(w in key for w in skip_words):
            continue
        if len(key) < 4:
            continue
        seen.add(key)
        deduped.append(c)

    return deduped


def scrape_slevaren_members() -> list[dict]:
    """Scrape Czech Foundry Association member directory."""
    results = []
    try:
        soup = fetch_page(SLEVAREN_URL)

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 1:
                name = cells[0].get_text(strip=True)
                if not name or len(name) < 3:
                    continue
                domain = ""
                if len(cells) >= 2:
                    link = cells[1].find("a")
                    if link and link.get("href"):
                        domain = link["href"].replace("http://", "").replace("https://", "").rstrip("/")
                results.append({
                    "company_name": name,
                    "role": "association_member",
                    "company_domain": domain,
                })
    except Exception:
        pass

    if not results:
        results = _slevaren_fallback()

    return results


def _slevaren_fallback() -> list[dict]:
    """Hardcoded fallback list from svazslevaren.cz/slevarny (scraped April 2026)."""
    companies = [
        ("Alfe Brno, s. r. o.", "alfe.cz"), ("Almet, a. s.", "almet.cz"),
        ("Alucast, s. r. o.", "alucast.cz"), ("ALUMETALL CZ s.r.o.", "alumetall.cz"),
        ("Aluminium Group, a. s.", "aluminiumgroup.cz"), ("ALW INDUSTRY, s. r. o.", "alw.cz"),
        ("AS-CASTING s.r.o.", "as-casting.cz"), ("Beneš a Lát, a. s.", "benesalat.cz"),
        ("Brano, a. s.", "brano.cz"), ("ČKD Kutná Hora, a.s.", "ckdkh.cz"),
        ("ČZ, a. s., Divize Slévárna hliníku", "czas.cz"), ("ČZ, a. s., Divize Slévárna litiny", "czas.cz"),
        ("Ernst Leopold s.r.o.", "ernstleopold.cz"), ("EURAC s.r.o.", "matfoundrygroup.cz"),
        ("EUTIT, s. r. o.", "eutit.cz"), ("Explat s.r.o.", "explat.cz"),
        ("Focam spol. s r. o.", "focam.cz"), ("Hamag, spol. s r. o.", "hamag.cz"),
        ("IMT foundry s.r.o.", "imtf.cz"), ("Kdynium, a. s.", "kdynium.cz"),
        ("Kovolis Hedvikov, a. s.", "kovolis-hedvikov.cz"), ("Kovolit Česká, s. r. o.", "kovolitceska.cz"),
        ("Kovolit Modřice, a. s.", "kovolit.cz"), ("KOVOSVIT MAS, a.s.", "kovosvit.cz"),
        ("Královopolská slévárna, s. r. o.", "kpslevarna.cz"), ("Metalurgie Rumburk s.r.o.", "metalurgie.cz"),
        ("METAZ Týnec a.s.", "metaz.cz"), ("Metso Czech Republic s.r.o.", "metso.com"),
        ("MHS tlakové lití s.r.o.", "mhsliti.cz"), ("Moravia Tech, a.s.", "moraviatech.cz"),
        ("Motor-Jikov Slévárna litiny, a. s.", "mjsl.cz"), ("Piston Rings Komarov s.r.o.", "komapistonrings.com"),
        ("Power cast – ORTMANN, s. r. o.", "ortmann.cz"), ("PROMET FOUNDRY a.s.", "prometfoundry.cz"),
        ("RKL Slévárna, s.r.o.", "rklslevarna.cz"), ("S+C Alfanametal s.r.o.", "alfanametal.cz"),
        ("Slévárna Heunisch Brno, s.r.o.", "heunisch-guss.cz"), ("Slévárna Heunisch, s.r.o. Krásná", "heunisch-guss.cz"),
        ("Slévárna hliníku, s. r. o., Nový Bor", "slevarnahliniku.cz"),
        ("Slévárny Třinec, a. s.", "trz.cz"), ("Slovácké strojírny, a.s.", "sub.cz"),
        ("SLP, s. r. o.", "slevarnaslp.cz"), ("TATRA METALURGIE a.s.", "tatrametalurgie.cz"),
        ("Uneko, spol. s r. o.", "uneko.cz"), ("Unex, a. s.", "unex.cz"),
        ("UXA, s. r. o.", "uxa.cz"), ("VÍTKOVICE HEAVY MACHINERY a.s.", "vitkovice.cz"),
        ("Vítkovické slévárny, spol. s r. o.", "vitkovickeslevarny.cz"),
        ("VÚHŽ, a. s.", "vuhz.cz"), ("ZPS – Slévárna, a. s.", "sl.zps.cz"),
        ("ŽĎAS, a. s.", "zdas.cz"), ("Železárny Štěpánov, spol. s r.o.", "zelezarny.cz"),
    ]
    return [{"company_name": n, "role": "association_member", "company_domain": d} for n, d in companies]


def scrape_czgbc_members() -> list[dict]:
    """Scrape Czech Green Building Council members list."""
    results = []
    try:
        soup = fetch_page(CZGBC_URL)

        for heading in soup.find_all("h3"):
            name = heading.get_text(strip=True)
            if name and len(name) > 2 and len(name) < 150:
                domain = ""
                parent = heading.parent
                if parent:
                    link = parent.find("a", href=lambda h: h and "website" in str(h).lower())
                    if not link:
                        for a in parent.find_all("a"):
                            href = a.get("href", "")
                            if href and "czgbc" not in href and "contact" not in href:
                                domain = href.replace("http://", "").replace("https://", "").rstrip("/")
                                break
                    else:
                        domain = link["href"].replace("http://", "").replace("https://", "").rstrip("/")

                results.append({
                    "company_name": name,
                    "role": "association_member",
                    "company_domain": domain,
                })
    except Exception:
        pass

    skip_words = ("members", "menu", "search", "login", "cookie", "gdpr",
                  "membership type", "field of activity", "contact",
                  "exclusive member", "regular member", "associated member")
    deduped = []
    seen = set()
    for c in results:
        key = c["company_name"].lower().strip()
        if key in seen or any(w in key for w in skip_words):
            continue
        if len(key) < 3:
            continue
        seen.add(key)
        deduped.append(c)

    return deduped


def scrape_moravian_aviation() -> list[dict]:
    """Scrape Moravian Aviation Cluster member directory."""
    results = []
    try:
        soup = fetch_page(MORAVIAN_AVIATION_URL)

        for a_tag in soup.find_all("a"):
            href = a_tag.get("href", "")
            if "/firma/" in href:
                name = a_tag.get_text(strip=True)
                if name and len(name) > 2:
                    results.append({
                        "company_name": name,
                        "role": "association_member",
                        "company_domain": "",
                    })

        if not results:
            for card in soup.find_all(["div", "article"]):
                classes = " ".join(card.get("class", []))
                if "card" in classes or "member" in classes or "firma" in classes:
                    heading = card.find(["h2", "h3", "h4"])
                    if heading:
                        name = heading.get_text(strip=True)
                        if name and len(name) > 2:
                            results.append({
                                "company_name": name,
                                "role": "association_member",
                                "company_domain": "",
                            })
    except Exception:
        pass

    if not results:
        results = _moravian_aviation_fallback()

    seen = set()
    deduped = []
    for c in results:
        key = c["company_name"].lower().strip()
        skip_words = ("menu", "home", "kontakt", "cookie", "moravský letecký")
        if key in seen or any(w in key for w in skip_words) or len(key) < 3:
            continue
        seen.add(key)
        deduped.append(c)

    return deduped


def _moravian_aviation_fallback() -> list[dict]:
    """Hardcoded fallback from czech-aerospace.cz member page."""
    names = [
        "DI industrial spol. s r.o.", "FK system – povrchové úpravy s.r.o.",
        "LASER CENTRUM CZ, s.r.o.", "Preteq CNC Solutions s.r.o.",
        "VŠB – TECHNICKÁ UNIVERZITA OSTRAVA", "Vysoké učení technické v Brně",
        "Ústav fyziky materiálů AV ČR, v. v. i.",
    ]
    return [{"company_name": n, "role": "association_member", "company_domain": ""} for n in names]


def scrape_hkcr_brno() -> list[dict]:
    """Scrape Brno regional Chamber of Commerce member list."""
    results = []
    try:
        soup = fetch_page(HKCR_BRNO_URL)

        for heading in soup.find_all("h4"):
            name = heading.get_text(strip=True)
            if not name or len(name) < 3 or len(name) > 120:
                continue
            parent = heading.parent
            domain = ""
            if parent:
                www_link = parent.find("a", href=lambda h: h and ("http" in str(h) or "www" in str(h)))
                if www_link:
                    href = www_link.get("href", "")
                    domain = href.replace("http://", "").replace("https://", "").rstrip("/")

            ico = ""
            if parent:
                text = parent.get_text()
                ico_match = re.search(r"IČO:\s*(\d+)", text)
                if ico_match:
                    ico = ico_match.group(1)

            results.append({
                "company_name": name,
                "role": "association_member",
                "company_domain": domain,
                "ico": ico,
            })
    except Exception:
        pass

    skip_words = ("domů", "členství", "seznam", "navigace", "menu",
                  "právnické osoby", "fyzické osoby", "města")
    deduped = []
    seen = set()
    for c in results:
        key = c["company_name"].lower().strip()
        if key in seen or any(w in key for w in skip_words) or len(key) < 3:
            continue
        seen.add(key)
        deduped.append(c)

    return deduped


def scrape_all_associations() -> dict:
    """Scrape all association member directories.

    Returns:
        Dict with results from all association sources.
    """
    sps = scrape_sps_members()
    spcr = scrape_spcr_members()
    slevaren = scrape_slevaren_members()
    czgbc = scrape_czgbc_members()
    aviation = scrape_moravian_aviation()
    hkcr = scrape_hkcr_brno()

    return {
        "success": True,
        "sps": {"count": len(sps), "companies": sps},
        "spcr": {"count": len(spcr), "companies": spcr},
        "slevaren": {"count": len(slevaren), "companies": slevaren},
        "czgbc": {"count": len(czgbc), "companies": czgbc},
        "aviation": {"count": len(aviation), "companies": aviation},
        "hkcr": {"count": len(hkcr), "companies": hkcr},
        "total": len(sps) + len(spcr) + len(slevaren) + len(czgbc) + len(aviation) + len(hkcr),
    }
