"""Autodesk competitor product mapping for ZoomInfo technographics matching.

Used by the lead scorer to identify competitive displacement opportunities.
"""

AUTODESK_PRODUCTS = {
    "AutoCAD", "Revit", "Inventor", "Fusion", "Fusion 360",
    "Civil 3D", "3ds Max", "Maya", "Navisworks", "BIM Collaborate",
    "Autodesk Construction Cloud", "ACC", "AutoCAD LT",
    "AutoCAD Plant 3D", "AutoCAD Architecture", "AutoCAD Electrical",
    "AutoCAD Mechanical", "AutoCAD MEP", "AutoCAD Map 3D",
    "Advance Steel", "Robot Structural Analysis",
    "Moldflow", "VRED", "Alias", "PowerInspect", "PowerMill",
    "FeatureCAM", "Netfabb", "InfraWorks", "Recap", "ReCap Pro",
    "Shotgun", "ShotGrid", "Flow Production Tracking",
    "Arnold", "Flame", "Smoke",
}

COMPETITORS_BY_SEGMENT = {
    "AEC": {
        "Trimble": ["SketchUp", "Tekla", "Trimble Connect", "Trimble Realworks"],
        "Bentley Systems": ["MicroStation", "OpenBuildings", "ProjectWise", "SYNCHRO", "iTwin"],
        "Nemetschek": ["Allplan", "Vectorworks", "Bluebeam", "Solibri"],
        "Graphisoft": ["ArchiCAD", "BIMx", "BIMcloud"],
        "Bricsys": ["BricsCAD"],
        "ESRI": ["ArcGIS"],
    },
    "D&M": {
        "Dassault Systemes": ["SolidWorks", "CATIA", "ENOVIA", "DELMIA", "3DEXPERIENCE"],
        "Siemens Digital Industries": ["Siemens NX", "Solid Edge", "Teamcenter", "Tecnomatix"],
        "PTC": ["Creo", "Windchill", "ThingWorx", "Onshape"],
        "Ansys": ["Ansys Mechanical", "Ansys Fluent"],
        "Hexagon": ["VISI", "WORKNC", "EDGECAM"],
        "Mastercam": ["Mastercam"],
    },
    "M&E": {
        "Maxon": ["Cinema 4D", "ZBrush", "Redshift"],
        "SideFX": ["Houdini"],
        "Foundry": ["Nuke", "Mari", "Katana"],
        "Unity Technologies": ["Unity"],
        "Epic Games": ["Unreal Engine"],
        "Blender Foundation": ["Blender"],
        "Pixar": ["RenderMan"],
    },
}

ALL_COMPETITOR_PRODUCTS = set()
PRODUCT_TO_COMPETITOR = {}
PRODUCT_TO_SEGMENT = {}

for segment, competitors in COMPETITORS_BY_SEGMENT.items():
    for company, products in competitors.items():
        for product in products:
            ALL_COMPETITOR_PRODUCTS.add(product.lower())
            PRODUCT_TO_COMPETITOR[product.lower()] = company
            PRODUCT_TO_SEGMENT[product.lower()] = segment

ALL_COMPETITOR_COMPANIES = set()
for segment, competitors in COMPETITORS_BY_SEGMENT.items():
    for company in competitors:
        ALL_COMPETITOR_COMPANIES.add(company.lower())

INDUSTRY_KEYWORDS = {
    "AEC": [
        "construction", "architecture", "engineering", "building", "infrastructure",
        "civil", "structural", "surveying", "geospatial", "BIM", "stavebnictvi",
        "architektura", "geodesie", "real estate", "facility",
    ],
    "D&M": [
        "manufacturing", "automotive", "aerospace", "machinery", "industrial",
        "production", "engineering", "mechanical", "CNC", "robotics",
        "vyroba", "strojirenstvi", "prumysl", "plastics", "mold",
    ],
    "M&E": [
        "media", "entertainment", "film", "game", "gaming", "animation",
        "visual effects", "VFX", "broadcast", "studio",
    ],
}


def detect_industry_segment(industry: str = None, description: str = None) -> str:
    """Guess the Autodesk segment (AEC, D&M, M&E) from industry text."""
    if not industry and not description:
        return "unknown"
    text = f"{industry or ''} {description or ''}".lower()
    scores = {"AEC": 0, "D&M": 0, "M&E": 0}
    for segment, keywords in INDUSTRY_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text:
                scores[segment] += 1
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "unknown"


def find_competitor_products_in_tech_stack(tech_stack: list) -> list:
    """Given a list of tech/product names, return competitor matches."""
    matches = []
    for tech in tech_stack:
        tech_lower = tech.lower().strip()
        for comp_product in ALL_COMPETITOR_PRODUCTS:
            if comp_product in tech_lower or tech_lower in comp_product:
                matches.append({
                    "product": tech,
                    "competitor": PRODUCT_TO_COMPETITOR.get(comp_product, "Unknown"),
                    "segment": PRODUCT_TO_SEGMENT.get(comp_product, "Unknown"),
                })
                break
    return matches


PRODUCT_NORMALIZE = {
    "autocad lt": "AutoCAD LT",
    "acdlt": "AutoCAD LT",
    "autocad": "AutoCAD",
    "autocad - including specialized toolsets": "AutoCAD",
    "revit": "Revit",
    "rvt": "Revit",
    "revit lt": "Revit LT",
    "autocad revit lt suite": "Revit LT",
    "rvtlts": "Revit LT",
    "inventor professional": "Inventor",
    "invprosa": "Inventor",
    "inventor": "Inventor",
    "fusion": "Fusion",
    "fusion 360": "Fusion",
    "fusion - legacy": "Fusion",
    "fusion - legacy 2024": "Fusion",
    "fusion - startup": "Fusion",
    "fusion for design": "Fusion",
    "fusion for manufacturing": "Fusion",
    "f360": "Fusion",
    "fusion contributor": "Fusion",
    "fusion with powermill standard": "Fusion + PowerMill",
    "fusion with powermill ultimate": "Fusion + PowerMill",
    "fusion with powershape": "Fusion + PowerShape",
    "fusion with powerinspect": "Fusion + PowerInspect",
    "fusion with netfabb": "Fusion + Netfabb",
    "fusion with netfabb standard": "Fusion + Netfabb",
    "fusion - with featurecam": "Fusion + FeatureCAM",
    "fusion - with featurecam standard": "Fusion + FeatureCAM",
    "fusion with eagle premium": "Fusion + EAGLE",
    "fusion with eagle standard": "Fusion + EAGLE",
    "fusion design extension": "Fusion Design Ext",
    "fusion manufacturing extension": "Fusion Mfg Ext",
    "fusion simulation extension": "Fusion Sim Ext",
    "fusion manage": "Fusion Manage",
    "fusion manage extension": "Fusion Manage Ext",
    "civil 3d": "Civil 3D",
    "3ds max": "3ds Max",
    "3ds max - for indie users": "3ds Max",
    "3dsmxiu": "3ds Max",
    "maya": "Maya",
    "maya - for indie users": "Maya",
    "navisworks manage": "Navisworks",
    "navisworks simulate": "Navisworks",
    "advance steel": "Advance Steel",
    "aec collection": "AEC Collection",
    "aeccol": "AEC Collection",
    "architecture engineering & construction collection": "AEC Collection",
    "product design & mfg collection": "PDMC",
    "product design & manufacturing collection": "PDMC",
    "autodesk product design & manufacturing collection": "PDMC",
    "media & entertainment collection": "M&E Collection",
    "bim collaborate": "BIM Collaborate",
    "bim collaborate pro": "BIM Collaborate Pro",
    "recap pro": "ReCap Pro",
    "vault professional": "Vault",
    "vault office": "Vault",
    "forma": "Forma",
    "forma build": "Forma",
    "forma build 550": "Forma",
    "forma data management": "Forma",
    "forma design collaboration": "Forma",
    "forma for model management": "Forma",
    "docs": "ACC Docs",
    "docs for aec collection": "ACC Docs",
    "build - 550": "ACC Build",
    "build - unlimited": "ACC Build",
    "flex": "Flex",
    "flexaccess": "Flex",
    "moldflow insight - legacy": "Moldflow",
    "moldflow synergy": "Moldflow",
    "moldflow - compute service": "Moldflow",
    "powerinspect - standard": "PowerInspect",
    "fabrication camduct": "Fabrication CAMduct",
    "motion builder": "MotionBuilder",
    "eagle - premium": "EAGLE",
    "eagle - standard": "EAGLE",
    "workshop xr": "Workshop XR",
    "alias concept": "Alias",
    "alias surface": "Alias",
    "autocad web": "AutoCAD Web",
    "autocad - mobile app legacy": "AutoCAD Mobile",
    "autocad lt for mac": "AutoCAD LT",
    "aps free": "APS",
    "aps pay as you go": "APS",
    "cfd - ultimate": "CFD",
}

AEC_PRODUCTS = {"Revit", "Civil 3D", "Navisworks", "Advance Steel",
                "InfraWorks", "Forma", "BIM Collaborate", "BIM Collaborate Pro",
                "ACC Docs", "ACC Build", "Fabrication CAMduct", "AEC Collection",
                "AutoCAD Architecture", "AutoCAD MEP", "AutoCAD Map 3D"}

DM_PRODUCTS = {"Inventor", "Fusion", "PowerMill", "PowerInspect", "FeatureCAM",
               "Moldflow", "Netfabb", "Vault", "VRED", "Alias", "PDMC",
               "AutoCAD Mechanical", "AutoCAD Electrical", "AutoCAD Plant 3D",
               "Fusion + PowerMill", "Fusion + PowerShape", "Fusion + PowerInspect",
               "Fusion + FeatureCAM", "Fusion + Netfabb", "Fusion + EAGLE",
               "Fusion Mfg Ext", "Fusion Sim Ext", "Fusion Manage"}

ME_PRODUCTS = {"Maya", "3ds Max", "Arnold", "Flame", "Smoke",
               "MotionBuilder", "M&E Collection", "Flow Production Tracking"}

UPSELL_PATHS_BY_SEGMENT = {
    "AEC": {
        "AutoCAD LT": [
            ("AutoCAD (full)", "AutoCAD 2026: 11x faster file open, 4x faster startup, AI Smart Blocks"),
            ("AEC Collection", "Full BIM workflow — Revit 2026 GPU-accelerated views + Civil 3D + Navisworks"),
        ],
        "AutoCAD": [
            ("AEC Collection", "Add Revit 2026 (GPU-accelerated BIM) + Civil 3D + Navisworks"),
            ("Forma", "Forma Building Design: real-time daylight/carbon analysis, direct Revit export"),
        ],
        "Revit LT": [
            ("Revit (full)", "Full Revit 2026: GPU-accelerated views, ReCap mesh integration, worksharing"),
            ("AEC Collection", "Complete AEC workflow with AutoCAD 2026 (11x faster), Civil 3D, Navisworks"),
        ],
        "Revit": [
            ("AEC Collection", "Bundle with AutoCAD 2026, Civil 3D, Navisworks — unified BIM platform"),
            ("BIM Collaborate Pro", "Cloud-based design coordination and clash detection (now Forma platform)"),
            ("Forma", "Forma Building Design: schematic design with analysis before detailed BIM"),
        ],
        "Civil 3D": [
            ("AEC Collection", "Bundle with Revit 2026 GPU-accelerated views, AutoCAD, Navisworks"),
            ("InfraWorks", "Conceptual infrastructure design and visualization"),
        ],
        "Navisworks": [
            ("AEC Collection", "Bundle with Revit 2026, AutoCAD, Civil 3D for full BIM coordination"),
            ("BIM Collaborate Pro", "Cloud coordination on Forma platform replacing desktop Navisworks"),
        ],
        "Advance Steel": [
            ("AEC Collection", "Bundle with Revit 2026, AutoCAD, Navisworks for multi-discipline"),
        ],
        "AEC Collection": [
            ("Forma", "Forma Building Design: cloud schematic design with daylight/carbon analysis"),
            ("BIM Collaborate Pro", "Cloud coordination, clash detection on unified Forma platform"),
            ("ACC Build", "Forma Build: field management, quality, safety — 70+ updates in 2026"),
            ("Flex", "Token access for occasional D&M or M&E tools"),
        ],
        "Flex": [
            ("AEC Collection", "Dedicated AEC seats if Revit/Civil 3D usage is consistent"),
        ],
    },
    "D&M": {
        "AutoCAD LT": [
            ("Inventor", "Step up to parametric 3D mechanical design with Inventor-to-Fusion cloud bridge"),
            ("Fusion", "Cloud CAD/CAM/CAE with AI Assistant — text-to-3D geometry, real-time cloud BOM"),
            ("PDMC", "Full D&M bundle saves ~EUR 1,985/yr vs standalone: Inventor + AutoCAD + Fusion + Vault"),
        ],
        "AutoCAD": [
            ("PDMC", "Add Inventor + Vault + Fusion — saves ~EUR 1,985/yr vs standalone, AI-powered workflows"),
            ("Inventor", "Parametric 3D mechanical design with cloud bridge to Fusion simulation/CAM"),
            ("Fusion", "Cloud-native CAD/CAM/CAE with AI Assistant and real-time cloud BOM collaboration"),
        ],
        "Inventor": [
            ("PDMC", "Bundle saves ~EUR 1,985/yr: adds AutoCAD Mech, Vault, Fusion with AI Assistant"),
            ("Fusion Mfg Ext", "5-axis CAM, sheet nesting, additive manufacturing + AI toolpath automation"),
            ("Vault", "Data management and revision control for Inventor files"),
            ("Fusion Sim Ext", "Generative design, FEA, thermal analysis — all within Fusion"),
        ],
        "Fusion": [
            ("Fusion Mfg Ext", "5-axis CAM, sheet nesting, additive manufacturing + automated machining"),
            ("Fusion Sim Ext", "Generative design, FEA, thermal analysis, injection molding simulation"),
            ("Fusion Manage Ext", "Cloud PLM and lifecycle management with real-time BOM collaboration"),
            ("PDMC", "Full D&M bundle if they also need Inventor + AutoCAD Mechanical"),
        ],
        "Fusion + PowerMill": [
            ("Fusion Mfg Ext", "Integrated 5-axis CAM directly in Fusion"),
            ("PDMC", "Full D&M bundle for broader design-to-manufacturing coverage"),
        ],
        "Fusion + PowerShape": [
            ("Fusion Mfg Ext", "Add 5-axis CAM and nesting to the Fusion workflow"),
            ("PDMC", "Full D&M bundle with Inventor, Vault, and AutoCAD Mechanical"),
        ],
        "Moldflow": [
            ("Fusion Sim Ext", "Integrated injection molding simulation in Fusion"),
            ("PDMC", "Full D&M bundle for broader design-to-manufacturing coverage"),
        ],
        "Vault": [
            ("PDMC", "Full D&M bundle with Inventor, Fusion, AutoCAD Mechanical"),
            ("Fusion Manage Ext", "Cloud-based PLM extending desktop Vault"),
        ],
        "PDMC": [
            ("Fusion Mfg Ext", "Advanced 5-axis CAM, nesting, additive manufacturing"),
            ("Fusion Sim Ext", "FEA simulation, generative design, thermal analysis"),
            ("Moldflow", "Injection molding simulation for plastics/composites"),
            ("Flex", "Token access for occasional AEC or M&E tools"),
        ],
        "EAGLE": [
            ("Fusion + EAGLE", "Unified ECAD+MCAD in single platform"),
        ],
        "Flex": [
            ("PDMC", "Dedicated D&M seats if Inventor/Fusion usage is consistent"),
            ("Fusion", "Cloud-native CAD/CAM/CAE as primary design platform"),
        ],
    },
    "M&E": {
        "AutoCAD LT": [
            ("3ds Max", "3ds Max 2026: Smart Bevel, Wonder 3D AI previsualization"),
            ("M&E Collection", "Full M&E bundle: Maya + 3ds Max + Arnold + Golaem crowd sim"),
        ],
        "3ds Max": [
            ("M&E Collection", "Bundle with Maya 2026, Arnold, MotionBuilder + Golaem crowd sim"),
        ],
        "Maya": [
            ("M&E Collection", "Bundle with 3ds Max, Arnold, MotionBuilder + Golaem crowd sim"),
        ],
        "M&E Collection": [
            ("Flow Production Tracking", "Production pipeline management for multi-shot projects"),
            ("Flex", "Token access for occasional AEC or D&M tools"),
        ],
        "Flex": [
            ("M&E Collection", "Dedicated M&E seats if Maya/3ds Max usage is consistent"),
        ],
    },
    "unknown": {
        "AutoCAD LT": [
            ("AutoCAD (full)", "Full AutoCAD unlocks 3D, customization, and toolsets"),
            ("PDMC", "D&M bundle: Inventor + AutoCAD + Fusion + Vault"),
            ("AEC Collection", "AEC bundle: Revit + Civil 3D + Navisworks"),
        ],
        "AutoCAD": [
            ("PDMC", "Add Inventor + Vault for design-to-manufacturing workflow"),
            ("AEC Collection", "Add Revit + Civil 3D for full BIM workflow"),
        ],
        "Flex": [
            ("PDMC", "Dedicated D&M seats if manufacturing usage is consistent"),
            ("AEC Collection", "Dedicated AEC seats if design/BIM usage is consistent"),
        ],
    },
}

UPSELL_PATHS = {
    product: paths
    for segment_paths in UPSELL_PATHS_BY_SEGMENT.values()
    for product, paths in segment_paths.items()
}


def normalize_products(raw_products: str) -> list:
    """Normalize a comma-separated product string into canonical names."""
    if not raw_products:
        return []
    seen = set()
    result = []
    for p in raw_products.split(","):
        p = p.strip()
        if not p:
            continue
        canonical = PRODUCT_NORMALIZE.get(p.lower(), p)
        if canonical.lower() not in seen:
            seen.add(canonical.lower())
            result.append(canonical)
    return result


def _infer_segment_from_products(normalized: list) -> str:
    """Infer the most likely Autodesk segment from the products a company owns."""
    normalized_lower = {p.lower() for p in normalized}
    aec_count = len(normalized_lower & {p.lower() for p in AEC_PRODUCTS})
    dm_count = len(normalized_lower & {p.lower() for p in DM_PRODUCTS})
    me_count = len(normalized_lower & {p.lower() for p in ME_PRODUCTS})
    if aec_count + dm_count + me_count == 0:
        return "unknown"
    best = max([("AEC", aec_count), ("D&M", dm_count), ("M&E", me_count)],
               key=lambda x: x[1])
    return best[0] if best[1] > 0 else "unknown"


def recommend_upsell(current_products: str, industry_segment: str = None) -> list:
    """Generate prioritized upsell recommendations based on current product
    portfolio AND industry segment.

    The segment drives which collection/extension path is recommended first.
    Returns a list of dicts: [{product, reason, priority}]
    """
    normalized = normalize_products(current_products)
    normalized_lower = {p.lower() for p in normalized}
    segment = industry_segment or "unknown"
    if segment in ("MFG",):
        segment = "D&M"

    if not normalized:
        seg_map = {
            "AEC": {"product": "AEC Collection", "reason": "Full AEC workflow — Revit, Civil 3D, AutoCAD, Navisworks"},
            "D&M": {"product": "PDMC", "reason": "Full D&M workflow — Inventor, Fusion, AutoCAD, Vault"},
            "M&E": {"product": "M&E Collection", "reason": "Full M&E workflow — Maya, 3ds Max, Arnold"},
        }
        if segment in seg_map:
            return [{**seg_map[segment], "priority": 1}]
        return [{"product": "Flex", "reason": "Token-based access to explore Autodesk portfolio", "priority": 1}]

    if segment == "unknown":
        segment = _infer_segment_from_products(normalized)

    has_collection = any(p in normalized_lower for p in ("aec collection", "pdmc", "m&e collection"))
    segment_paths = UPSELL_PATHS_BY_SEGMENT.get(segment, UPSELL_PATHS_BY_SEGMENT["unknown"])

    recommendations = []
    seen_recs = set()

    for product in normalized:
        paths = segment_paths.get(product, UPSELL_PATHS.get(product, []))
        for target, reason in paths:
            target_lower = target.lower()
            if target_lower in normalized_lower or target_lower in seen_recs:
                continue
            seen_recs.add(target_lower)
            pri = 1 if "collection" in target_lower or "pdmc" in target_lower else 2
            recommendations.append({"product": target, "reason": reason, "priority": pri})

    if not has_collection and not recommendations:
        current_aec = normalized_lower & {p.lower() for p in AEC_PRODUCTS}
        current_dm = normalized_lower & {p.lower() for p in DM_PRODUCTS}
        current_me = normalized_lower & {p.lower() for p in ME_PRODUCTS}

        seg_bundle_map = {
            "AEC": ("AEC Collection", current_aec, "aec collection"),
            "D&M": ("PDMC", current_dm, "pdmc"),
            "M&E": ("M&E Collection", current_me, "m&e collection"),
        }
        if segment in seg_bundle_map:
            bname, btools, bkey = seg_bundle_map[segment]
            if btools and bkey not in seen_recs:
                recommendations.append({
                    "product": bname,
                    "reason": f"Bundle {len(btools)} standalone {segment} tools into the collection",
                    "priority": 1,
                })
                seen_recs.add(bkey)

        for seg_key, (bname, btools, bkey) in seg_bundle_map.items():
            if seg_key != segment and btools and bkey not in seen_recs:
                recommendations.append({
                    "product": bname,
                    "reason": f"Bundle {len(btools)} standalone {seg_key} tools into the collection",
                    "priority": 2,
                })
                seen_recs.add(bkey)

    if segment == "AEC" and not has_collection:
        if not any(p.lower() in ("forma", "forma build") for p in normalized):
            if "forma" not in seen_recs:
                recommendations.append({
                    "product": "Forma",
                    "reason": "Early-stage design and sustainability analysis for AEC",
                    "priority": 3,
                })

    if segment == "D&M":
        if not any("mfg ext" in p.lower() or "manufacturing ext" in p.lower() for p in normalized):
            if "fusion mfg ext" not in seen_recs and any(p.lower() in ("fusion", "inventor") for p in normalized):
                recommendations.append({
                    "product": "Fusion Mfg Ext",
                    "reason": "5-axis CAM, nesting, additive — top revenue driver for D&M",
                    "priority": 2,
                })
                seen_recs.add("fusion mfg ext")

    if "flex" not in normalized_lower and "flex" not in seen_recs and not has_collection:
        recommendations.append({
            "product": "Flex",
            "reason": "Token-based access to broader Autodesk portfolio",
            "priority": 4,
        })

    recommendations.sort(key=lambda x: x["priority"])
    return recommendations[:5]


def calculate_whitespace(current_products: str, segment: str = None) -> dict:
    """Calculate what Autodesk products a client could still buy.

    Returns a dict with the whitespace analysis.
    """
    if not current_products:
        return {"current": [], "potential": list(AUTODESK_PRODUCTS), "gap_ratio": 1.0}

    current = {p.strip() for p in current_products.split(",")}
    current_lower = {p.lower() for p in current}

    potential = []
    for product in AUTODESK_PRODUCTS:
        if product.lower() not in current_lower:
            potential.append(product)

    total = len(AUTODESK_PRODUCTS)
    gap_ratio = len(potential) / total if total > 0 else 0

    return {
        "current": list(current),
        "potential": potential[:10],
        "gap_ratio": gap_ratio,
    }
