"""
llm_analyse.py — local LLM version using Ollama (free, unlimited)
==================================================================
Reads commodity_tweet_impact.csv and analyses each impactful tweet
using a locally running LLM via Ollama. No API key, no cost, no limits.

Setup (one time):
  brew install ollama
  ollama pull deepseek-r1:7b
  ollama serve          ← run this in a separate terminal, keep it open

Then run:
  python3.11 llm_analyse.py
"""

import json
import re
import time
import logging
import urllib.request
import urllib.error
import pandas as pd

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

INPUT_CSV      = "commodity_tweet_impact.csv"
OUTPUT_CSV     = "commodity_tweet_impact.csv"   # updated in-place
HIGHLIGHTS_CSV = "poster_highlights.csv"

# Ollama settings
OLLAMA_URL  = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "deepseek-r1:1.5b"   # change to "deepseek-r1:7b" for larger model

# Only analyse tweets where absolute 30-min price change exceeds this
MIN_IMPACT_PCT = 0.05

TOP_N = 20   # how many highlights to export for the poster

TICKER_NAMES = {
    "CL=F": "Crude Oil (WTI)",
    "GC=F": "Gold",
    "SI=F": "Silver",
    "NG=F": "Natural Gas",
    "HG=F": "Copper",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# OLLAMA CALL
# ─────────────────────────────────────────────

SYSTEM_PROMPT = (
    "You are a commodity futures markets analyst specialising in social media market impact analysis. "
    "You will be given a tweet and the commodity price move that occurred in the 30 minutes following it. "
    "Your job is to classify the tweet, explain the causal mechanism, assess your confidence, and predict short-term direction. "
    "You MUST output ONLY a single valid JSON object. "
    "No markdown. No code fences. No explanation. No text before or after. "
    "The output must start with { and end with }.\n\n"
    "Allowed values:\n"
    "  sentiment: BULLISH | BEARISH | NEUTRAL (toward the commodity)\n"
    "  tweet_type: GEOPOLITICAL | SUPPLY_SHOCK | POLICY | SANCTIONS | MONETARY_POLICY | MARKET_COMMENTARY | NEWS | OTHER\n"
    "  influence_mechanism: SUPPLY_DEMAND | GEOPOLITICAL_RISK | MONETARY_POLICY | SAFE_HAVEN | RISK_SENTIMENT | SPECULATION | INDIRECT\n"
    "  confidence: HIGH (tweet clearly and directly relates to this commodity) | MEDIUM (indirect relationship) | LOW (weak or coincidental link)\n"
    "  prediction: BULLISH | BEARISH | NEUTRAL\n\n"
    "EXAMPLE INPUT:\n"
    "Tweet by @OPECSecretariat: 'OPEC+ agrees to extend production cuts of 1.66mb/d through end of 2024'\n"
    "Crude Oil (WTI) futures rose by 1.24% in 30 minutes.\n\n"
    "EXAMPLE OUTPUT:\n"
    '{"sentiment":"BULLISH","tweet_type":"SUPPLY_SHOCK","influence_mechanism":"SUPPLY_DEMAND",'
    '"causal_chain":"OPEC+ announcing an extension of production cuts directly reduces the global crude oil supply outlook. Fewer barrels in the market tighten supply against steady demand, pushing prices upward as traders price in reduced future supply.",'
    '"price_move_explanation":"The 1.24% rise reflects immediate buying pressure as traders repositioned long on crude following confirmation of sustained supply restriction. The official OPEC+ source gave the announcement high credibility.",'
    '"confidence":"HIGH",'
    '"prediction":"BULLISH",'
    '"prediction_reasoning":"Sustained production cuts will continue to support oil prices in the near term, with further upside if compliance among member states remains high."}'
)

def call_ollama(prompt: str) -> str:
    payload = json.dumps({
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 900,
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        OLLAMA_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result["message"]["content"]
    except urllib.error.URLError:
        raise ConnectionError(
            "Cannot connect to Ollama. Make sure it's running:\n"
            "  ollama serve\n"
            "(open a new terminal, run that command, keep it open)"
        )


def extract_json(text: str) -> dict:
    """Extract the first JSON object found in a string."""
    # DeepSeek sometimes wraps output in <think> tags — strip them
    if "<think>" in text:
        text = text.split("</think>")[-1]

    # Strip markdown code fences (```json ... ``` or ``` ... ```)
    text = re.sub(r"```(?:json)?", "", text)

    # Find the first { ... } block
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON found in response: {text[:200]}")

    return json.loads(text[start:end])


def analyse_tweet(tweet_text: str, account: str, ticker: str,
                  pct_change: float, tweet_time: str) -> dict:

    commodity = TICKER_NAMES.get(ticker, ticker)
    direction = "rose" if pct_change > 0 else "fell"
    abs_pct   = abs(pct_change)

    prompt = f"""Tweet by @{account} at {tweet_time} UTC:
"{tweet_text}"

{commodity} futures {direction} by {abs_pct:.4f}% in the 30 minutes after this tweet.

Analyse this tweet-price event and output a single JSON object with these fields:
- sentiment: was this tweet BULLISH, BEARISH, or NEUTRAL toward {commodity}?
- tweet_type: GEOPOLITICAL, SUPPLY_SHOCK, POLICY, SANCTIONS, MONETARY_POLICY, MARKET_COMMENTARY, NEWS, or OTHER
- influence_mechanism: SUPPLY_DEMAND, GEOPOLITICAL_RISK, MONETARY_POLICY, SAFE_HAVEN, RISK_SENTIMENT, SPECULATION, or INDIRECT
- causal_chain: 2 sentences — what specific content in this tweet would lead traders to buy or sell {commodity}?
- price_move_explanation: 2 sentences — why did {commodity} specifically {direction} by {abs_pct:.4f}%?
- confidence: HIGH if the tweet clearly and directly relates to {commodity}, MEDIUM if indirect, LOW if the link is weak or coincidental
- prediction: BULLISH, BEARISH, or NEUTRAL for {commodity} over the next few hours
- prediction_reasoning: 1 sentence on expected {commodity} price direction based on this tweet's implications"""

    try:
        raw = call_ollama(prompt)
        result = extract_json(raw)
        return {
            "llm_sentiment":        result.get("sentiment", ""),
            "llm_tweet_type":       result.get("tweet_type", ""),
            "llm_mechanism":        result.get("influence_mechanism", ""),
            "llm_reasoning":        result.get("causal_chain", ""),
            "llm_price_move":       result.get("price_move_explanation", ""),
            "llm_confidence":       result.get("confidence", ""),
            "llm_prediction":       result.get("prediction", ""),
            "llm_pred_reason":      result.get("prediction_reasoning", ""),
        }
    except (ValueError, json.JSONDecodeError) as e:
        log.warning(f"JSON parse error for @{account}: {e}")
        return {k: "PARSE_ERROR" for k in
                ["llm_sentiment","llm_tweet_type","llm_mechanism","llm_reasoning",
                 "llm_price_move","llm_confidence","llm_prediction","llm_pred_reason"]}
    except ConnectionError as e:
        log.error(str(e))
        raise
    except Exception as e:
        log.error(f"Error for @{account}: {e}")
        return {k: "ERROR" for k in
                ["llm_sentiment","llm_tweet_type","llm_mechanism","llm_reasoning",
                 "llm_price_move","llm_confidence","llm_prediction","llm_pred_reason"]}


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run():
    # Check Ollama is reachable before starting
    try:
        call_ollama("Say OK")
        log.info(f"Ollama connected — using model: {OLLAMA_MODEL}")
    except ConnectionError as e:
        print(f"\n❌  {e}")
        return

    log.info(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV, parse_dates=["post_time_utc"])

    # Add LLM columns if missing
    for col in ["llm_sentiment","llm_tweet_type","llm_mechanism","llm_reasoning",
                "llm_price_move","llm_confidence","llm_prediction","llm_pred_reason"]:
        if col not in df.columns:
            df[col] = ""

    # Select rows to analyse
    mask = (
        df["pct_change_30m"].notna() &
        (df["pct_change_30m"].abs() >= MIN_IMPACT_PCT) &
        (df["llm_sentiment"].isna() | (df["llm_sentiment"] == ""))
    )

    to_analyse = df[mask]
    total = len(to_analyse)
    log.info(f"Tweets to analyse: {total}  (|Δ%| ≥ {MIN_IMPACT_PCT}%)")

    if total == 0:
        log.info("Nothing to analyse — run twitter_stock_scraper.py first.")
        return

    for i, (idx, row) in enumerate(to_analyse.iterrows(), 1):
        log.info(f"[{i}/{total}]  @{row['username']}  {row['ticker']}  "
                 f"{row['pct_change_30m']:+.4f}%")

        result = analyse_tweet(
            tweet_text = str(row["content"]),
            account    = str(row["username"]),
            ticker     = str(row["ticker"]),
            pct_change = float(row["pct_change_30m"]),
            tweet_time = str(row["post_time_utc"])[:16],
        )

        for col, val in result.items():
            df.at[idx, col] = val

        df.to_csv(OUTPUT_CSV, index=False)  # save after every tweet

    log.info(f"\n✅  Analysis complete — {OUTPUT_CSV} updated")

    # ── Poster highlights ────────────────────────────────────────────────

    # Add commodity name column if missing
    if "commodity" not in df.columns:
        df["commodity"] = df["ticker"].map(TICKER_NAMES).fillna(df["ticker"])

    done = df[
        df["llm_sentiment"].notna() &
        ~df["llm_sentiment"].isin(["", "ERROR", "PARSE_ERROR"])
    ].copy()

    if done.empty:
        log.info("No successfully analysed tweets to export.")
        return

    done["abs_pct"] = done["pct_change_30m"].abs()

    highlights = (
        done
        .sort_values("abs_pct", ascending=False)
        .drop_duplicates(subset=["tweet_id", "ticker"])
        .head(TOP_N)
        [[
            "username", "ticker", "commodity", "post_time_utc",
            "content", "pct_change_5m", "pct_change_15m", "pct_change_30m",
            "like_count", "retweet_count", "post_url",
            "llm_sentiment", "llm_tweet_type", "llm_mechanism",
            "llm_confidence", "llm_reasoning", "llm_price_move",
            "llm_prediction", "llm_pred_reason"
        ]]
    )

    highlights.to_csv(HIGHLIGHTS_CSV, index=False)
    log.info(f"✅  Top {len(highlights)} poster highlights → {HIGHLIGHTS_CSV}")

    # ── Influence ranking ────────────────────────────────────────────────

    print("\n" + "="*70)
    print("ACCOUNT INFLUENCE RANKING  (mean |Δ%| at 30 min)")
    print("="*70)

    ranking = (
        done
        .groupby(["username", "ticker", "commodity"])
        .agg(
            n_tweets     = ("tweet_id",       "count"),
            mean_abs_pct = ("abs_pct",        "mean"),
            pct_bullish  = ("llm_sentiment",
                            lambda x: round((x == "BULLISH").mean() * 100, 1)),
            top_type     = ("llm_tweet_type",
                            lambda x: x.mode()[0] if not x.mode().empty else ""),
        )
        .round({"mean_abs_pct": 4})
        .sort_values("mean_abs_pct", ascending=False)
        .reset_index()
    )
    print(ranking.to_string(index=False))

    print("\n" + "="*70)
    print("TOP 10 MOST IMPACTFUL TWEETS")
    print("="*70)

    for _, row in highlights.head(10).iterrows():
        arrow = "▲" if row["pct_change_30m"] > 0 else "▼"
        print(f"\n{arrow} {row['pct_change_30m']:+.4f}% | @{row['username']} → {row['commodity']}")
        print(f"  Type : {row['llm_tweet_type']}  |  Sentiment: {row['llm_sentiment']}")
        print(f"  Tweet: {str(row['content'])[:120]}...")
        print(f"  Why  : {row['llm_reasoning']}")
        print(f"  Pred : {row['llm_prediction']} — {row['llm_pred_reason']}")


if __name__ == "__main__":
    run()
