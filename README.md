# Social Media & Commodity Markets — Research Pipeline

**Dataset period:** 2026-03-01 → 2026-04-29  
**Commodities tracked:** Crude Oil (WTI), Gold, Silver, Natural Gas, Copper

---

## Overview

This project investigates whether tweets from high-profile financial, political, and energy accounts correlate with short-term commodity futures price movements. The pipeline has three stages:

```
twitter_stock_scraper.py  →  commodity_tweet_impact.csv
          ↓
    llm_analyse.py        →  commodity_tweet_impact.csv  (LLM columns added)
                            →  poster_highlights.csv
```

---

## Files

| File | Purpose |
|------|---------|
| `twitter_stock_scraper.py` | Scrapes tweets, matches to commodities, fetches price data |
| `llm_analyse.py` | Runs each impactful tweet through a local LLM for causal analysis |
| `commodity_tweet_impact.csv` | Main dataset — one row per tweet-commodity event |
| `commodity_tweet_summary.csv` | Aggregated stats per account-ticker pair |
| `poster_highlights.csv` | Top 20 most impactful tweets with full LLM analysis |
| `cookies.json` | Twitter/X session cookies (generated on first login, do not share) |

---

## Stage 1 — `twitter_stock_scraper.py`

### What it does

1. Authenticates to Twitter/X using the `twikit` library (cookie-based, no official API key needed).
2. Fetches up to `MAX_TWEETS_PER_ACCOUNT` (200) recent tweets from each account in the watchlist.
3. Filters tweets to the configured date window (`SINCE_DATE` / `UNTIL_DATE`).
4. Applies keyword matching to decide which commodity ticker(s) each tweet is relevant to.
5. For each matched tweet-ticker pair, fetches 5-minute intraday price data via `yfinance` and records the price at the tweet time, and 5, 15, and 30 minutes later.
6. Merges new rows with any existing `commodity_tweet_impact.csv` (keeping previously collected LLM analysis intact).
7. Produces `commodity_tweet_summary.csv` with aggregated stats per account-ticker pair.

### Key configuration constants

| Constant | Default | Meaning |
|----------|---------|---------|
| `SINCE_DATE` | 2026-03-01 | Earliest tweet date to include |
| `UNTIL_DATE` | 2026-04-29 | Latest tweet date to include |
| `MAX_TWEETS_PER_ACCOUNT` | 200 | Max tweets fetched per account per run |
| `OUTPUT_CSV` | `commodity_tweet_impact.csv` | Main output file |
| `SUMMARY_CSV` | `commodity_tweet_summary.csv` | Aggregated summary output |
| `SKIP_ACCOUNTS` | (set of usernames) | Accounts to skip on the current run (rate limit management) |

### Account watchlist & keyword matching

The script monitors **21 accounts** across five categories:

- **Heads of state / government:** `realDonaldTrump`, `POTUS`, `KremlinRussia_E`, `MFA_Russia`, `NicolasMaduro`, `Jokowi`
- **OPEC & energy companies:** `OPECSecretariat`, `GazpromEN`, `Shell`, `ExxonMobil`, `Chevron`
- **US policy institutions:** `USTreasury`, `federalreserve`
- **Macro economists & precious metals analysts:** `PeterSchiff`, `ZoltanPozsar`, `LukeGromen`, `RaoulGMI`, `elerianm`, `WallStSilver`, `JimRogersSays`
- **Financial news outlets:** `JavierBlas`, `Oilpricecom`, `EnergyIntel`, `KitcoNewsNOW`, `GoldSeek`, `SilverDoctors`, `TFMetals`, `Reuters`, `ReutersBiz`, `markets`, `BloombergEnergy`, `WSJmarkets`, `FT`, `CNBC`

Each account has a per-commodity keyword list in `ACCOUNT_STOCK_RULES`. A tweet is matched to a ticker only if its lowercased text (with URLs removed) contains at least one of that ticker's keywords. One tweet can match multiple commodities, creating multiple rows.

Keywords cover:
- **Direct terms:** `oil`, `gold`, `silver`, `gas`, `copper`
- **Geopolitical triggers:** `iran`, `hormuz`, `missile`, `drone`, `sanctions`, `nuclear`, `irgc`
- **Ceasefire/conflict language:** `ceasefire`, `cease-fire`, `deadline`, `ultimatum`, `breach`, `truce`, `violated agreement`
- **Policy language:** `fed`, `rate`, `inflation`, `monetary`, `tariff`, `trade`

### Rate limit management — `SKIP_ACCOUNTS`

Twitter/X rate-limits unauthenticated-style requests. To spread scraping across multiple sessions without re-fetching already-collected accounts, add usernames to the `SKIP_ACCOUNTS` set. The script prints `⏭ skipped` for those accounts and moves on. Clear the set to re-scrape everyone from scratch.

### Market hours detection

Commodity futures (CME Globex) trade **Sunday 23:00 UTC through Friday 22:00 UTC**, with a daily maintenance break 22:00–23:00 UTC. The `is_commodity_market_open()` function flags each tweet accordingly. Tweets outside market hours are still stored (with `market_open = False`) but receive `None` for all price columns since there is no live price to fetch.

### Deduplication / merge logic

On every run the script reads the existing CSV (if present), concatenates new rows, and drops duplicates by `(tweet_id, ticker)` keeping the **first** occurrence. This preserves any LLM analysis columns already filled in by `llm_analyse.py` — re-running the scraper never overwrites completed analysis.

### Dependencies

```
pip install twikit yfinance pandas
```

> **Note:** `twikit` from PyPI may be outdated. If you see errors related to `ClientTransaction` (webpack chunk format) or missing `User` fields (`pinned_tweet_ids_str`, `withheld_in_countries`), apply the patches described in the project notes to the installed library files.

---

## Stage 2 — `llm_analyse.py`

### What it does

1. Reads `commodity_tweet_impact.csv`.
2. Filters rows where `|pct_change_30m| >= 0.05%` and where the LLM columns are not yet filled.
3. Sends each qualifying tweet to a locally running **Ollama** instance (DeepSeek model) for causal analysis.
4. Parses the JSON response and writes 8 new columns back into the CSV row by row (so progress is preserved if interrupted).
5. Exports `poster_highlights.csv` with the top 20 most impactful tweets and their full analysis.
6. Prints an account influence ranking and a top-10 impactful tweets summary to the terminal.

### Setup (one time)

```bash
brew install ollama
ollama pull deepseek-r1:1.5b   # or deepseek-r1:7b for higher quality
ollama serve                   # keep this running in a separate terminal
```

### Key configuration constants

| Constant | Default | Meaning |
|----------|---------|---------|
| `OLLAMA_MODEL` | `deepseek-r1:1.5b` | Local model to use |
| `MIN_IMPACT_PCT` | `0.05` | Minimum absolute 30-min % change to trigger analysis |
| `TOP_N` | `20` | Number of tweets in the highlights export |

### How the LLM prompt works

Each tweet is sent with a structured system prompt that instructs the model to act as a commodity futures analyst and return **only** a valid JSON object with exactly these fields:

| JSON field | Type | Description |
|------------|------|-------------|
| `sentiment` | `BULLISH / BEARISH / NEUTRAL` | Direction of the tweet toward the commodity |
| `tweet_type` | enum | Broad category: `GEOPOLITICAL`, `SUPPLY_SHOCK`, `POLICY`, `SANCTIONS`, `MONETARY_POLICY`, `MARKET_COMMENTARY`, `NEWS`, `OTHER` |
| `influence_mechanism` | enum | How price transmission works: `SUPPLY_DEMAND`, `GEOPOLITICAL_RISK`, `MONETARY_POLICY`, `SAFE_HAVEN`, `RISK_SENTIMENT`, `SPECULATION`, `INDIRECT` |
| `causal_chain` | 2-sentence string | Why this tweet would cause traders to buy or sell |
| `price_move_explanation` | 2-sentence string | Why the specific % move occurred |
| `confidence` | `HIGH / MEDIUM / LOW` | How directly the tweet relates to the commodity |
| `prediction` | `BULLISH / BEARISH / NEUTRAL` | LLM's short-term directional call |
| `prediction_reasoning` | 1-sentence string | Rationale for the prediction |

The model output is cleaned of any `<think>` tags (a DeepSeek quirk) and markdown fences before JSON parsing. If parsing fails the row is marked `PARSE_ERROR` and skipped on future runs until manually cleared.

### Running

```bash
# In one terminal:
ollama serve

# In another:
python3.11 llm_analyse.py
```

Progress is printed as `[i/total] @account  TICKER  +0.1234%` and the CSV is written after every single tweet so you can safely interrupt and resume.

---

## Output CSV Files

### `commodity_tweet_impact.csv` — main dataset

One row per **tweet × commodity** event. A single tweet that matches three commodities will appear as three rows.

| Column | Type | Description |
|--------|------|-------------|
| `username` | str | Twitter/X screen name (no @) |
| `ticker` | str | Yahoo Finance ticker (`CL=F`, `GC=F`, `SI=F`, `NG=F`, `HG=F`) |
| `tweet_id` | str | Unique tweet ID |
| `post_url` | str | Direct link to the tweet on x.com |
| `post_time_utc` | datetime | Tweet timestamp in UTC |
| `market_open` | bool | Whether CME Globex was open when the tweet was posted |
| `content` | str | Full tweet text |
| `like_count` | int | Likes at time of scraping |
| `retweet_count` | int | Retweets at time of scraping |
| `reply_count` | int | Replies at time of scraping |
| `quote_count` | int | Quote tweets at time of scraping |
| `price_at_post` | float | Futures price at tweet time (nearest 5-min bar) |
| `price_5m` | float | Futures price 5 minutes after tweet |
| `price_15m` | float | Futures price 15 minutes after tweet |
| `price_30m` | float | Futures price 30 minutes after tweet |
| `pct_change_5m` | float | `(price_5m − price_at_post) / price_at_post × 100` |
| `pct_change_15m` | float | Same for 15 min |
| `pct_change_30m` | float | Same for 30 min — **primary impact metric** |
| `llm_sentiment` | str | LLM: `BULLISH / BEARISH / NEUTRAL` |
| `llm_tweet_type` | str | LLM: tweet category |
| `llm_mechanism` | str | LLM: transmission mechanism |
| `llm_reasoning` | str | LLM: causal chain explanation |
| `llm_price_move` | str | LLM: explanation of the specific price move |
| `llm_confidence` | str | LLM: `HIGH / MEDIUM / LOW` |
| `llm_prediction` | str | LLM: short-term directional prediction |
| `llm_pred_reason` | str | LLM: one-sentence prediction rationale |

Rows where `market_open = False` will have `None` for all price and pct_change columns. LLM columns are empty string until `llm_analyse.py` has processed the row, or `PARSE_ERROR` / `ERROR` if processing failed.

---

### `commodity_tweet_summary.csv` — per account-ticker aggregates

One row per `(username, ticker)` pair, covering only price-matched rows.

| Column | Description |
|--------|-------------|
| `username` | Account screen name |
| `ticker` | Commodity ticker |
| `tweets_analysed` | Number of price-matched tweet-events |
| `avg_pct_change_5m` | Mean % price change at 5 min across all matched tweets |
| `avg_pct_change_15m` | Mean % price change at 15 min |
| `avg_pct_change_30m` | Mean % price change at 30 min |
| `median_pct_change_30m` | Median % price change at 30 min |
| `std_pct_change_30m` | Standard deviation of 30-min % change |
| `avg_likes` | Mean like count of matched tweets |
| `avg_retweets` | Mean retweet count of matched tweets |

Sorted by `|avg_pct_change_30m|` descending. Useful for a quick ranking of which account-commodity combinations show the strongest average correlation.

---

### `poster_highlights.csv` — top 20 most impactful tweets

Exported by `llm_analyse.py`. Contains the 20 rows with the highest `|pct_change_30m|` among successfully LLM-analysed tweets (no `ERROR` / `PARSE_ERROR`). Duplicates by `tweet_id + ticker` are removed so the same tweet does not appear twice for the same commodity.

Columns are a subset of `commodity_tweet_impact.csv`:

`username`, `ticker`, `commodity`, `post_time_utc`, `content`, `pct_change_5m`, `pct_change_15m`, `pct_change_30m`, `like_count`, `retweet_count`, `post_url`, `llm_sentiment`, `llm_tweet_type`, `llm_mechanism`, `llm_confidence`, `llm_reasoning`, `llm_price_move`, `llm_prediction`, `llm_pred_reason`

---

## Running the full pipeline

```bash
# 1. Scrape tweets and fetch prices
python3.11 twitter_stock_scraper.py

# 2. Run LLM analysis (ollama must be running in another terminal)
python3.11 llm_analyse.py
```

Each step is safe to re-run. The scraper merges and deduplicates; the analyser skips already-processed rows. But you only need to add your Twitter login credentials and possibly update the `cookies.json` with your logged in cookies, by using a cookie extractor Google extension.

---

## Limitations

- **Correlation, not causation.** Price moves in the 30-minute window after a tweet may be caused by many simultaneous factors. The keyword match and LLM reasoning are heuristic, not causal proof.
- **LLM quality.** `deepseek-r1:1.5b` is a small model run locally. Outputs occasionally contain classification errors or verbose non-JSON text (handled by the `PARSE_ERROR` fallback). A larger model (`7b` or cloud-based) would produce more reliable results.
- **Market hours gaps.** Tweets posted outside CME Globex hours (Saturday, early Sunday, Friday evenings) have no price data. Significant announcements at weekends are captured as text but cannot be price-matched.
- **Rate limits.** Twitter/X aggressively rate-limits scraping. Use `SKIP_ACCOUNTS` to spread collection across sessions and wait 15–20 minutes between runs if you hit HTTP 429 errors.
