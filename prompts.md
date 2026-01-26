# Polymarket Analytics - Development Prompts

This document contains the prompts used to build and iterate on the Polymarket Analytics project.

---

## Phase 1: Initial Build & Core Features

### Market Tracking & Data Collection
> Build a local-first analytics tool for Polymarket prediction markets with market tracking, accuracy analysis, black swan detection, and large moves monitoring.

### Outcome Simulation Feature
> Add an outcome simulation feature that can access any historical Polymarket data and simulate investment outcomes.

---

## Phase 2: Data Fetching Investigation

### Liquidity vs Volume Issue
> What I wanted is for you to double check the logic of data fetching - because I should see more markets with such liquidity (for example, crypto prediction markets as they have have liquidity and many prediction events every day that resolved fast)

**Discovery**: Resolved markets have $0 liquidity because liquidity is drained after settlement. Volume is the meaningful metric for resolved markets.

---

## Phase 3: Major Refactor - Volume-Based Analytics

### Switch from Liquidity to Volume
> Can you redo the analytics web app part by fetching and analysing not by liquidity but by volume (example landing page, markets section (sort by volume not liquidity), biggest movers section isn't properly working -> there were big moves in last 24 hours, black swans as well happened (add conditions to capture if 14 days before resolution the outcome certainty was > 70% yes or no and then in one day it dropped to less than 50%))

**Changes made**:
- Updated config: `min_liquidity_usd` → `min_volume_usd` (default $100k)
- Updated polymarket_client.py: sort by `volumeNum`
- Updated analytics.py: default sort to "volume", improved movers detection with `max_swing`
- Updated ingestion.py: new `_detect_black_swan()` method with 14-day rule
- Updated all templates: dashboard, markets, movers, black_swans

---

## Phase 4: Black Swan Detection Issues

### No Records Found
> Black Swan Events page there are no records check if you didn't make a mistake

**Investigation**: Database had 0 resolved markets - black swans only detected when tracked markets resolve.

### API-Based Black Swan Search
> Can you search for black swans by not limiting yourself to 500 markets tracked - go outside and find black swans in last 60 days of polymarket data

**Implementation**:
- Added `find_black_swans_from_api()` method to polymarket_client.py
- Added `_check_price_reversal()` helper checking prices at 14, 7, 3 days before resolution
- Updated server.py to support `source=api` parameter
- Updated black_swans.html template with source toggle

**Result**: Found 15 black swan events including TikTok sale (1.4% early odds), Epstein files markets, sports underdogs.

---

## Phase 5: Performance Optimization

### Slow Page Load
> Should this sub-page load slow now?

**Options presented**:
- A: Add in-memory caching (30 min TTL)
- B: Parallelize price history checks
- C: Both caching + parallelization

### Implementation Choice
> c

**Implementation**:
- Added 30-minute in-memory cache for black swan results
- Parallelized price checks using `asyncio.gather()` in batches of 20
- Result: First load ~16 seconds (down from 60+), cached loads instant

---

## Phase 6: UI/UX Improvements

### Detection Criteria Display
> Add in black swan page a description of conditions we use to select black swan events

**Added**:
- Detection criteria box showing: winning outcome < 40%, min volume $100k, last 60 days
- Updated "What is a Black Swan?" section to "Why Track Black Swans?"

### Column Explanations
> Add explanations to black swan column categories what they exactly mean

**Added**:
- Column guide box explaining Early Prob, Winner, Volume columns
- Tooltips on table headers
- CSS styling for hoverable headers

### Date Columns
> Add date of early prob and column of prob once black swan event happened with date as well

**Added**:
- Early Date column (when low probability was recorded)
- Final Prob column with resolution date
- Updated `_check_price_reversal()` to return timestamps
- Color-coded badges: red for upset probability, green for winner

---

## Summary Requests

### Product Summary
> Can you generate a short summary of approaches we tried and product iterations we did

> Provide summary of the 1st product version (before simulations feature)

> Can you combine both features into one product description (because it's one web app)

### Value Proposition
> Tell me why this project is useful, interesting

> Provide an answer in one sentence

**One-sentence answer**: This project helps you understand when and why prediction markets fail by tracking historical accuracy, detecting dramatic reversals (black swans), and simulating what would have happened if you had bet on markets at any point in time.

---

## Technical Fixes Along the Way

1. **Python 3.9 syntax error**: Changed `tuple[bool, float | None]` to `tuple` (union syntax not supported)

2. **CLOB API time interval limit**: Changed from 30-day window to checking at specific days (14, 7, 3) with 1-day windows each

3. **Resolved markets $0 liquidity**: Switched all filtering from liquidity to volume throughout the app
