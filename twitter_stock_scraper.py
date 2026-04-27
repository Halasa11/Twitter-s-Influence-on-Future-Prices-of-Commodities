import asyncio
import warnings
warnings.filterwarnings("ignore")

import json
import os
import re
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List

import pandas as pd
import yfinance as yf
from twikit import Client


# =========================================
# 1. CONFIG
# =========================================

COOKIES_FILE = "cookies.json"

SINCE_DATE = pd.Timestamp("2026-03-01", tz="UTC")
UNTIL_DATE = pd.Timestamp("2026-04-29", tz="UTC")

MAX_TWEETS_PER_ACCOUNT = 200

OUTPUT_CSV  = "commodity_tweet_impact.csv"
SUMMARY_CSV = "commodity_tweet_summary.csv"


# =========================================
# 2. ACCOUNTS & KEYWORD RULES
#
# A tweet is only matched to a ticker if its
# text contains at least one listed keyword.
# =========================================

# ✅ Accounts already scraped — skip on next run.
# Clear this set once you want to re-scrape them.
SKIP_ACCOUNTS = {
    "KremlinRussia_E",
    "MFA_Russia",
    "NicolasMaduro",
    "Jokowi",
    "OPECSecretariat",
    "GazpromEN",
    "Shell",
    "ExxonMobil",
    "Chevron",
    "USTreasury",
    "federalreserve",
    "PeterSchiff",
    "ZoltanPozsar",
    "LukeGromen",
    "RaoulGMI",
    "elerianm",
    "WallStSilver",
    "JimRogersSays",
    "JavierBlas",
    "Oilpricecom",
    "EnergyIntel",
    "KitcoNewsNOW",
    "GoldSeek",
    "SilverDoctors",
    "TFMetals",
    "Reuters",
    "ReutersBiz",
    "markets",
    "BloombergEnergy",
    "WSJmarkets",
    "FT",
    "CNBC",
}

ACCOUNT_STOCK_RULES: Dict[str, Dict[str, List[str]]] = {

    # ── HEADS OF STATE ─────────────────────────────────────────────────────
    "realDonaldTrump": {
        "CL=F": ["oil", "opec", "energy", "iran", "iranian", "saudi", "venezuela",
                 "pipeline", "tariff", "sanctions", "russia", "barrel",
                 "hormuz", "strait", "missile", "missiles", "ballistic",
                 "drone", "drones", "tanker", "navy", "naval", "warship",
                 "airstrike", "air strike", "tehran", "irgc", "nuclear",
                 "enrichment", "persian gulf", "red sea", "houthi", "houthis",
                 "hezbollah", "escalation", "retaliation", "middle east",
                 "israel", "blockade", "refinery", "petroleum", "chokepoint",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "GC=F": ["gold", "fed", "dollar", "inflation", "reserve", "rate",
                 "war", "conflict", "crisis", "geopolitical", "escalation",
                 "missile", "iran", "nuclear", "middle east", "safe haven",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
        "SI=F": ["silver", "gold", "inflation", "dollar"],
        "NG=F": ["gas", "lng", "energy", "pipeline", "russia", "europe",
                 "iran", "hormuz", "persian gulf"],
        "HG=F": ["copper", "china", "tariff", "manufacturing", "trade"],
    },
    "POTUS": {
        "CL=F": ["oil", "energy", "opec", "iran", "iranian", "sanctions",
                 "pipeline", "hormuz", "strait", "missile", "missiles",
                 "ballistic", "drone", "drones", "tanker", "navy", "naval",
                 "warship", "carrier", "airstrike", "tehran", "irgc",
                 "nuclear", "enrichment", "persian gulf", "red sea",
                 "houthi", "houthis", "hezbollah", "escalation",
                 "retaliation", "middle east", "israel", "blockade",
                 "petroleum", "chokepoint", "refinery",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "GC=F": ["gold", "inflation", "fed", "reserve", "war", "conflict",
                 "crisis", "geopolitical", "iran", "nuclear", "safe haven",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
        "NG=F": ["gas", "lng", "energy", "pipeline", "iran", "hormuz"],
    },
    "KremlinRussia_E": {
        "CL=F": ["oil", "energy", "opec", "barrel", "petroleum",
                 "iran", "iranian", "hormuz", "sanctions", "middle east",
                 "missile", "drone", "tanker", "persian gulf", "escalation",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "truce",
                 "negotiations", "peace deal", "diplomatic"],
        "NG=F": ["gas", "nordstream", "pipeline", "lng", "gazprom", "energy",
                 "iran", "hormuz", "persian gulf"],
        "GC=F": ["gold", "ruble", "reserve", "sanctions", "war", "conflict",
                 "geopolitical", "iran", "nuclear", "escalation",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
    },
    "MFA_Russia": {
        "CL=F": ["oil", "sanctions", "energy", "iran", "iranian", "opec",
                 "hormuz", "strait", "missile", "drone", "tanker",
                 "persian gulf", "red sea", "houthi", "middle east",
                 "escalation", "retaliation", "nuclear", "tehran",
                 "irgc", "blockade", "petroleum",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "NG=F": ["gas", "pipeline", "nordstream", "lng", "iran", "hormuz"],
        "GC=F": ["gold", "sanctions", "reserve", "war", "conflict",
                 "geopolitical", "iran", "nuclear", "escalation",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
    },
    "NicolasMaduro": {
        "CL=F": ["oil", "petroleum", "opec", "barrel", "energy", "pdvsa",
                 "iran", "sanctions"],
    },
    "Jokowi": {
        "HG=F": ["copper", "nickel", "mining", "mineral", "metal"],
        "GC=F": ["gold", "mining", "mineral"],
    },

    # ── OPEC & ENERGY COMPANIES ────────────────────────────────────────────
    "OPECSecretariat": {
        "CL=F": ["oil", "opec", "production", "cut", "barrel", "market",
                 "supply", "demand", "quota", "meeting",
                 "iran", "iranian", "hormuz", "strait", "sanctions",
                 "disruption", "geopolitical", "middle east", "persian gulf",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "truce",
                 "negotiations", "peace deal", "diplomatic"],
        "NG=F": ["gas", "lng", "energy", "iran", "hormuz"],
    },
    "GazpromEN": {
        "NG=F": ["gas", "pipeline", "supply", "lng", "export", "europe",
                 "delivery", "iran", "hormuz"],
        "CL=F": ["oil", "energy", "production", "iran", "sanctions"],
    },
    "Shell": {
        "CL=F": ["oil", "crude", "production", "offshore", "barrel",
                 "iran", "hormuz", "tanker", "red sea", "houthi",
                 "middle east", "sanctions", "disruption"],
        "NG=F": ["gas", "lng", "supply", "energy", "iran", "hormuz"],
    },
    "ExxonMobil": {
        "CL=F": ["oil", "crude", "production", "refinery", "barrel",
                 "iran", "hormuz", "tanker", "sanctions", "middle east",
                 "disruption", "red sea"],
        "NG=F": ["gas", "lng", "energy", "iran"],
    },
    "Chevron": {
        "CL=F": ["oil", "crude", "production", "offshore", "barrel",
                 "iran", "hormuz", "tanker", "sanctions", "middle east",
                 "disruption", "red sea"],
        "NG=F": ["gas", "lng", "energy", "iran"],
    },

    # ── US POLICY / TREASURY / FED ─────────────────────────────────────────
    "USTreasury": {
        "GC=F": ["gold", "dollar", "reserve", "inflation", "debt", "bond",
                 "rate", "war", "conflict", "geopolitical", "safe haven"],
        "SI=F": ["silver", "dollar", "inflation"],
        "CL=F": ["sanctions", "oil", "iran", "iranian", "russia", "energy",
                 "hormuz", "petroleum", "nuclear", "tehran", "irgc",
                 "middle east", "blockade", "escalation"],
    },
    "federalreserve": {
        "GC=F": ["rate", "inflation", "interest", "monetary", "policy",
                 "gold", "dollar", "fed"],
        "SI=F": ["rate", "inflation", "silver"],
        "CL=F": ["inflation", "economy", "rate", "oil", "energy"],
    },

    # ── MACRO ECONOMISTS & GOLD/SILVER ADVOCATES ───────────────────────────
    "PeterSchiff": {
        "GC=F": ["gold", "dollar", "inflation", "fed", "rate", "bullion",
                 "oz", "precious", "buy gold", "gold price",
                 "war", "conflict", "iran", "geopolitical", "safe haven",
                 "crisis", "middle east", "missile", "nuclear"],
        "SI=F": ["silver", "gold", "inflation", "precious", "oz"],
    },
    "ZoltanPozsar": {
        "GC=F": ["gold", "dollar", "reserve", "commodity", "petrodollar",
                 "monetary", "geopolitical", "war", "iran", "safe haven"],
        "CL=F": ["oil", "commodity", "energy", "petrodollar", "iran",
                 "hormuz", "sanctions", "middle east"],
    },
    "LukeGromen": {
        "GC=F": ["gold", "dollar", "inflation", "reserve", "monetary", "fed",
                 "war", "conflict", "geopolitical", "iran", "safe haven",
                 "crisis", "middle east"],
        "SI=F": ["silver", "gold", "inflation"],
    },
    "RaoulGMI": {
        "GC=F": ["gold", "inflation", "macro", "dollar", "rate",
                 "war", "geopolitical", "iran", "safe haven", "crisis"],
        "SI=F": ["silver", "gold", "inflation"],
        "CL=F": ["oil", "energy", "macro", "commodity", "iran",
                 "hormuz", "middle east", "sanctions"],
    },
    "elerianm": {
        "GC=F": ["gold", "fed", "inflation", "rate", "dollar", "monetary",
                 "war", "geopolitical", "iran", "safe haven", "risk"],
        "CL=F": ["oil", "energy", "inflation", "commodity", "iran",
                 "hormuz", "middle east", "sanctions", "disruption"],
    },
    "WallStSilver": {
        "SI=F": ["silver", "$si", "ag", "precious", "stackers", "ounce"],
        "GC=F": ["gold", "precious", "bullion", "war", "safe haven",
                 "geopolitical", "iran", "crisis"],
    },
    "JimRogersSays": {
        "CL=F": ["oil", "commodity", "energy", "iran", "hormuz",
                 "middle east", "sanctions"],
        "GC=F": ["gold", "commodity", "inflation", "war", "geopolitical",
                 "iran", "safe haven"],
        "HG=F": ["copper", "commodity", "china", "metals"],
    },

    # ── ENERGY & COMMODITY JOURNALISTS ─────────────────────────────────────
    "JavierBlas": {
        "CL=F": ["oil", "crude", "opec", "barrel", "brent", "wti",
                 "energy", "supply", "demand", "production",
                 "iran", "iranian", "hormuz", "strait of hormuz",
                 "missile", "missiles", "ballistic", "drone", "drones",
                 "tanker", "tankers", "navy", "naval", "warship", "carrier",
                 "airstrike", "air strike", "tehran", "irgc",
                 "revolutionary guard", "nuclear", "enrichment", "uranium",
                 "persian gulf", "gulf", "red sea", "houthi", "houthis",
                 "hezbollah", "escalation", "retaliation", "middle east",
                 "israel", "blockade", "refinery", "petroleum",
                 "chokepoint", "shipping lane", "sanctions", "war",
                 "conflict", "attack", "strike", "bombing",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "NG=F": ["gas", "lng", "natural gas", "pipeline", "energy",
                 "iran", "hormuz", "persian gulf", "disruption"],
        "GC=F": ["gold", "commodity", "safe haven", "war", "geopolitical",
                 "ceasefire", "deadline", "ultimatum", "truce"],
    },
    "Oilpricecom": {
        "CL=F": ["oil", "crude", "opec", "barrel", "brent", "wti",
                 "supply", "production", "energy",
                 "iran", "iranian", "hormuz", "strait of hormuz",
                 "missile", "drone", "tanker", "navy", "naval",
                 "airstrike", "tehran", "irgc", "nuclear",
                 "persian gulf", "red sea", "houthi", "hezbollah",
                 "escalation", "retaliation", "middle east", "israel",
                 "blockade", "refinery", "sanctions", "war",
                 "attack", "strike", "chokepoint", "shipping",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "NG=F": ["gas", "lng", "natural gas", "iran", "hormuz",
                 "persian gulf"],
    },
    "EnergyIntel": {
        "CL=F": ["oil", "crude", "opec", "barrel", "energy", "supply",
                 "iran", "iranian", "hormuz", "strait of hormuz",
                 "missile", "drone", "tanker", "navy", "naval",
                 "airstrike", "tehran", "irgc", "nuclear",
                 "persian gulf", "red sea", "houthi", "hezbollah",
                 "escalation", "middle east", "israel", "blockade",
                 "refinery", "sanctions", "war", "attack",
                 "chokepoint", "shipping lane",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "NG=F": ["gas", "lng", "natural gas", "pipeline",
                 "iran", "hormuz", "persian gulf"],
    },
    "KitcoNewsNOW": {
        "GC=F": ["gold", "$gold", "oz", "bullion", "precious", "xau",
                 "price", "rally", "drop", "war", "conflict", "iran",
                 "geopolitical", "safe haven", "crisis", "missile",
                 "nuclear", "middle east", "escalation"],
        "SI=F": ["silver", "$silver", "oz", "precious", "xag",
                 "war", "safe haven", "geopolitical"],
    },
    "GoldSeek": {
        "GC=F": ["gold", "bullion", "oz", "precious", "xau",
                 "war", "conflict", "iran", "geopolitical",
                 "safe haven", "crisis", "middle east"],
        "SI=F": ["silver", "precious", "oz"],
    },
    "SilverDoctors": {
        "SI=F": ["silver", "oz", "precious", "ag", "xag"],
        "GC=F": ["gold", "precious", "bullion", "war", "safe haven",
                 "geopolitical", "iran", "crisis"],
    },
    "TFMetals": {
        "GC=F": ["gold", "silver", "comex", "precious", "oz", "bullion",
                 "war", "geopolitical", "safe haven", "iran",
                 "crisis", "middle east"],
        "SI=F": ["silver", "oz", "precious", "comex"],
    },

    # ── FINANCIAL NEWS OUTLETS ─────────────────────────────────────────────
    "Reuters": {
        "CL=F": ["oil", "crude", "opec", "barrel", "brent", "wti", "energy",
                 "iran", "iranian", "hormuz", "strait of hormuz",
                 "missile", "missiles", "ballistic", "drone", "drones",
                 "tanker", "tankers", "navy", "naval", "warship", "carrier",
                 "airstrike", "air strike", "tehran", "irgc",
                 "revolutionary guard", "nuclear", "enrichment", "uranium",
                 "persian gulf", "gulf", "red sea", "houthi", "houthis",
                 "hezbollah", "escalation", "retaliation", "middle east",
                 "israel", "blockade", "refinery", "petroleum",
                 "chokepoint", "shipping lane", "sanctions", "war",
                 "conflict", "attack", "strike",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "GC=F": ["gold", "precious", "bullion", "xau", "safe haven",
                 "war", "conflict", "geopolitical", "iran", "nuclear",
                 "crisis", "escalation", "middle east",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
        "SI=F": ["silver", "precious", "xag"],
        "NG=F": ["natural gas", "lng", "gas", "pipeline",
                 "iran", "hormuz", "persian gulf"],
        "HG=F": ["copper", "metal", "mining"],
    },
    "ReutersBiz": {
        "CL=F": ["oil", "crude", "opec", "barrel", "energy",
                 "iran", "iranian", "hormuz", "missile", "drone",
                 "tanker", "navy", "airstrike", "tehran", "nuclear",
                 "persian gulf", "red sea", "houthi", "hezbollah",
                 "escalation", "middle east", "israel", "sanctions",
                 "war", "chokepoint", "refinery",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "GC=F": ["gold", "precious", "bullion", "safe haven", "war",
                 "geopolitical", "iran", "crisis", "escalation",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
        "SI=F": ["silver", "precious"],
        "NG=F": ["gas", "lng", "pipeline", "iran", "hormuz"],
        "HG=F": ["copper", "metal", "mining"],
    },
    "markets": {
        "CL=F": ["oil", "crude", "opec", "barrel", "brent", "wti",
                 "iran", "hormuz", "missile", "drone", "tanker",
                 "navy", "airstrike", "tehran", "nuclear", "persian gulf",
                 "red sea", "houthi", "escalation", "middle east",
                 "sanctions", "war", "blockade", "chokepoint",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "GC=F": ["gold", "bullion", "precious", "safe haven", "war",
                 "geopolitical", "iran", "crisis", "escalation",
                 "middle east", "ceasefire", "cease-fire", "deadline",
                 "ultimatum", "48 hours", "truce", "negotiations"],
        "SI=F": ["silver", "precious"],
        "NG=F": ["gas", "lng", "natural gas", "iran", "hormuz"],
        "HG=F": ["copper", "metal"],
    },
    "BloombergEnergy": {
        "CL=F": ["oil", "crude", "barrel", "opec", "energy", "brent", "wti",
                 "iran", "iranian", "hormuz", "strait of hormuz",
                 "missile", "drone", "tanker", "navy", "naval",
                 "airstrike", "tehran", "irgc", "nuclear",
                 "persian gulf", "red sea", "houthi", "hezbollah",
                 "escalation", "middle east", "israel", "sanctions",
                 "war", "blockade", "chokepoint", "refinery", "petroleum",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "NG=F": ["gas", "lng", "natural gas", "pipeline",
                 "iran", "hormuz", "persian gulf"],
    },
    "WSJmarkets": {
        "CL=F": ["oil", "crude", "energy", "barrel",
                 "iran", "hormuz", "missile", "drone", "tanker",
                 "navy", "airstrike", "nuclear", "persian gulf",
                 "red sea", "houthi", "escalation", "middle east",
                 "sanctions", "war", "chokepoint",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "truce",
                 "negotiations", "peace deal", "diplomatic"],
        "GC=F": ["gold", "precious", "safe haven", "war", "geopolitical",
                 "iran", "crisis", "escalation",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "truce", "negotiations"],
        "HG=F": ["copper", "metal"],
    },
    "FT": {
        "CL=F": ["oil", "crude", "opec", "energy", "barrel",
                 "iran", "iranian", "hormuz", "missile", "drone",
                 "tanker", "navy", "airstrike", "tehran", "nuclear",
                 "persian gulf", "red sea", "houthi", "hezbollah",
                 "escalation", "middle east", "israel", "sanctions",
                 "war", "blockade", "chokepoint", "refinery",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "GC=F": ["gold", "precious", "inflation", "safe haven", "war",
                 "geopolitical", "iran", "crisis", "escalation",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
        "NG=F": ["gas", "lng", "energy", "iran", "hormuz", "persian gulf"],
    },
    "CNBC": {
        "CL=F": ["oil", "crude", "opec", "barrel", "energy",
                 "iran", "iranian", "hormuz", "missile", "drone",
                 "tanker", "navy", "airstrike", "tehran", "nuclear",
                 "persian gulf", "red sea", "houthi", "hezbollah",
                 "escalation", "middle east", "israel", "sanctions",
                 "war", "blockade", "chokepoint", "refinery",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "72 hours", "hours remaining",
                 "truce", "peace deal", "final warning", "peace talks",
                 "last chance", "diplomatic", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "ceasefire collapse", "violated agreement",
                 "agreement violated", "breach", "breached",
                 "broke the deal", "terms violated"],
        "GC=F": ["gold", "precious", "inflation", "safe haven", "war",
                 "geopolitical", "iran", "crisis", "escalation",
                 "ceasefire", "cease-fire", "deadline", "ultimatum",
                 "48 hours", "24 hours", "truce", "negotiations",
                 "ceasefire violation", "violating ceasefire",
                 "breaking ceasefire", "ceasefire broken",
                 "breach", "breached", "violated agreement"],
        "NG=F": ["gas", "lng", "energy", "iran", "hormuz"],
    },
}


# =========================================
# 3. COMMODITY TRADING HOURS
# CME Globex: Sun 23:00 UTC -> Fri 22:00 UTC
# Daily maintenance break: 22:00-23:00 UTC
# =========================================

def is_commodity_market_open(dt_utc: pd.Timestamp) -> bool:
    dt_utc  = pd.Timestamp(dt_utc).tz_convert("UTC")
    weekday = dt_utc.weekday()   # 0=Mon, 5=Sat, 6=Sun
    hour    = dt_utc.hour

    if weekday == 5:                # Saturday — fully closed
        return False
    if weekday == 6 and hour < 23:  # Sunday — opens 23:00 UTC
        return False
    if weekday == 4 and hour >= 22: # Friday — closes 22:00 UTC
        return False
    if hour == 22:                  # Daily maintenance 22:00-23:00 UTC
        return False

    return True


# =========================================
# 4. HELPERS
# =========================================

def clean_text(text: str) -> str:
    text = str(text).lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def match_tickers(username: str, text: str) -> List[str]:
    text_clean = clean_text(text)
    rules = ACCOUNT_STOCK_RULES.get(username, {})
    return [t for t, kws in rules.items() if any(kw in text_clean for kw in kws)]


def pct_change(start: Optional[float], end: Optional[float]) -> Optional[float]:
    if start is None or end is None or start == 0:
        return None
    return ((end - start) / start) * 100


def get_intraday_prices(ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    s = (start - pd.Timedelta(minutes=5)).tz_convert("UTC")
    e = (end   + pd.Timedelta(minutes=5)).tz_convert("UTC")
    df = yf.download(
        ticker,
        start=s.strftime("%Y-%m-%d"),
        end=(e + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="5m", progress=False,
        auto_adjust=False, prepost=False, threads=False,
    )
    if df.empty:
        return df
    df.index = df.index.tz_localize("UTC") if df.index.tz is None else df.index.tz_convert("UTC")
    return df.sort_index()


def nearest_price(df: pd.DataFrame, target: pd.Timestamp) -> Optional[float]:
    if df.empty:
        return None
    target = pd.Timestamp(target).tz_convert("UTC")
    cands  = df[df.index >= target]
    if cands.empty:
        return None
    val = cands.iloc[0]["Close"]
    return float(val.iloc[0] if isinstance(val, pd.Series) else val)


def parse_tweet_time(tweet) -> Optional[pd.Timestamp]:
    raw = getattr(tweet, "created_at", None)
    if raw is None:
        return None
    try:
        return pd.to_datetime(raw, utc=True)
    except Exception:
        pass
    try:
        return pd.to_datetime(raw, format="%a %b %d %H:%M:%S %z %Y", utc=True)
    except Exception:
        return None


# =========================================
# 5. LOGIN
# =========================================

# ── YOUR CREDENTIALS ──────────────────────────
USERNAME = ""
EMAIL    = ""
PASSWORD = ""
# ─────────────────────────────────────────────

async def get_client() -> Client:
    client = Client("en-US")

    if os.path.exists(COOKIES_FILE):
        print(f"Loading cookies from {COOKIES_FILE}")
        with open(COOKIES_FILE, "r") as f:
            raw = json.load(f)
        cookies = {c["name"]: c["value"] for c in raw} if isinstance(raw, list) else raw
        client.set_cookies(cookies)
    else:
        print("No cookies found — logging in with credentials...")
        await client.login(
            auth_info_1=USERNAME,
            auth_info_2=EMAIL,
            password=PASSWORD,
            cookies_file=COOKIES_FILE
        )
        print(f"Logged in and saved cookies to {COOKIES_FILE}")

    return client


# =========================================
# 6. FETCH TWEETS
# =========================================

async def fetch_user_tweets(client: Client, screen_name: str, max_tweets: int) -> List[dict]:
    rows = []
    try:
        user   = await client.get_user_by_screen_name(screen_name)
        tweets = await client.get_user_tweets(user.id, "Tweets", count=max_tweets)
    except Exception as e:
        print(f"  ✗ @{screen_name}: {e}")
        return rows

    count  = 0
    cursor = tweets
    while cursor is not None and count < max_tweets:
        for tweet in cursor:
            if count >= max_tweets:
                break
            created_at = parse_tweet_time(tweet)
            if created_at is None:
                continue
            text = getattr(tweet, "full_text", None) or getattr(tweet, "text", "")
            rows.append({
                "username":      screen_name,
                "tweet_id":      getattr(tweet, "id", None),
                "post_time_utc": created_at,
                "content":       text,
                "like_count":    getattr(tweet, "favorite_count", None),
                "retweet_count": getattr(tweet, "retweet_count", None),
                "reply_count":   getattr(tweet, "reply_count", None),
                "quote_count":   getattr(tweet, "quote_count", None),
                "post_url":      f"https://x.com/{screen_name}/status/{getattr(tweet, 'id', '')}",
            })
            count += 1
        if count >= max_tweets:
            break
        try:
            cursor = await cursor.next()
        except Exception:
            break

    return rows


# =========================================
# 7. MAIN
# =========================================

async def main():
    client = await get_client()

    print("\nCollecting tweets...\n")
    all_posts = []
    for username in ACCOUNT_STOCK_RULES:
        if username in SKIP_ACCOUNTS:
            print(f"  ⏭  @{username}: skipped (already collected)")
            continue
        posts = await fetch_user_tweets(client, username, MAX_TWEETS_PER_ACCOUNT)
        print(f"  ✓ @{username}: {len(posts)} tweets")
        all_posts.extend(posts)
        await asyncio.sleep(8)

    if not all_posts:
        print("No tweets collected.")
        return

    df = pd.DataFrame(all_posts)

    # Date filter
    df = df[(df["post_time_utc"] >= SINCE_DATE) & (df["post_time_utc"] <= UNTIL_DATE)].copy()
    if df.empty:
        print("No tweets in date range.")
        return

    # Market hours flag
    df["market_open"] = df["post_time_utc"].apply(is_commodity_market_open)
    print(f"\n{df['market_open'].sum()} tweets during commodity market hours, "
          f"{(~df['market_open']).sum()} outside (stored, not price-matched)")

    # Keyword matching -> expand to one row per tweet-ticker pair
    expanded = []
    for _, row in df.iterrows():
        for ticker in match_tickers(row["username"], row["content"]):
            expanded.append({**row.to_dict(), "ticker": ticker})

    if not expanded:
        print("No tweets matched keyword rules.")
        return

    matched = pd.DataFrame(expanded)
    print(f"Matched {len(matched)} tweet-commodity events across {matched['ticker'].nunique()} tickers\n")

    # Price lookup
    results     = []
    price_cache = {}

    for _, row in matched.iterrows():
        ticker  = row["ticker"]
        t0      = pd.Timestamp(row["post_time_utc"]).tz_convert("UTC")
        t30     = t0 + pd.Timedelta(minutes=30)
        is_open = bool(row["market_open"])

        if is_open:
            key = (ticker, t0.date())
            if key not in price_cache:
                try:
                    price_cache[key] = get_intraday_prices(ticker, t0, t30)
                except Exception as e:
                    print(f"  Price error {ticker} {t0.date()}: {e}")
                    price_cache[key] = pd.DataFrame()
            dfp = price_cache[key]
            p0  = nearest_price(dfp, t0)
            p5  = nearest_price(dfp, t0 + pd.Timedelta(minutes=5))
            p15 = nearest_price(dfp, t0 + pd.Timedelta(minutes=15))
            p30 = nearest_price(dfp, t30)
        else:
            p0 = p5 = p15 = p30 = None

        results.append({
            "username":          row["username"],
            "ticker":            ticker,
            "tweet_id":          row["tweet_id"],
            "post_url":          row["post_url"],
            "post_time_utc":     t0,
            "market_open":       is_open,
            "content":           row["content"],
            "like_count":        row["like_count"],
            "retweet_count":     row["retweet_count"],
            "reply_count":       row["reply_count"],
            "quote_count":       row.get("quote_count"),
            "price_at_post":     p0,
            "price_5m":          p5,
            "price_15m":         p15,
            "price_30m":         p30,
            "pct_change_5m":     pct_change(p0, p5),
            "pct_change_15m":    pct_change(p0, p15),
            "pct_change_30m":    pct_change(p0, p30),
            # Filled by llm_analyse.py
            "llm_sentiment":     "",
            "llm_tweet_type":    "",
            "llm_reasoning":     "",
            "llm_prediction":    "",
            "llm_pred_reason":   "",
        })

    out = pd.DataFrame(results)

    # Merge with existing data so old tweets + LLM analysis are preserved
    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV)
        print(f"  Loaded {len(existing)} existing records from {OUTPUT_CSV}")
        combined = pd.concat([existing, out], ignore_index=True)
        # Deduplicate: keep existing rows (which may have LLM columns filled)
        # by keeping the FIRST occurrence of each tweet_id + ticker pair
        combined = combined.drop_duplicates(subset=["tweet_id", "ticker"], keep="first")
        print(f"  After merge + dedup: {len(combined)} records ({len(combined) - len(existing):+d} new)")
        out = combined

    out.to_csv(OUTPUT_CSV, index=False)
    print(f"✅  Saved {len(out)} records to {OUTPUT_CSV}")

    # Summary
    valid = out.dropna(subset=["pct_change_30m"]).copy()
    if valid.empty:
        print("No price-matched results for summary.")
        return

    summary = (
        valid
        .groupby(["username", "ticker"], as_index=False)
        .agg(
            tweets_analysed       = ("tweet_id",        "count"),
            avg_pct_change_5m     = ("pct_change_5m",   "mean"),
            avg_pct_change_15m    = ("pct_change_15m",  "mean"),
            avg_pct_change_30m    = ("pct_change_30m",  "mean"),
            median_pct_change_30m = ("pct_change_30m",  "median"),
            std_pct_change_30m    = ("pct_change_30m",  "std"),
            avg_likes             = ("like_count",      "mean"),
            avg_retweets          = ("retweet_count",   "mean"),
        )
        .sort_values("avg_pct_change_30m", key=lambda s: s.abs(), ascending=False)
    )
    summary.to_csv(SUMMARY_CSV, index=False)
    print(f"✅  Saved summary to {SUMMARY_CSV}")
    print("\nTop results by absolute avg 30-min % change:")
    print(summary.head(20).to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
