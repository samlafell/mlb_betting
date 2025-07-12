# Action Network Integration

This directory contains utilities for working with Action Network APIs and data.

## 🔧 What This Does

The Action Network URL builder solves a key problem: **How to programmatically construct URLs for Action Network's game data endpoints**.

Action Network uses Next.js with dynamic build IDs that change when they deploy. Our utilities:

1. **Extract the current build ID** by monitoring network traffic on their sharp report page
2. **Construct valid game data URLs** by combining the build ID with game information
3. **Cache build IDs** to avoid repeated extraction

## 📂 Structure

```
action/
├── utils/
│   ├── __init__.py
│   ├── actionnetwork_build_extractor.py  # Extracts Next.js build ID
│   └── actionnetwork_url_builder.py      # Builds complete URLs
├── examples/
│   └── url_builder_demo.py               # Demo script
└── README.md                             # This file
```

## 🚀 Quick Start

### 1. Extract Build ID

```python
from action.utils.actionnetwork_build_extractor import extract_build_id

# Extract current build ID
build_id = extract_build_id()
print(f"Current build ID: {build_id}")
# Output: Current build ID: 5ewWa01cMp6swFO15XxC9
```

### 2. Construct Game URLs

```python
from action.utils.actionnetwork_build_extractor import ActionNetworkBuildExtractor

extractor = ActionNetworkBuildExtractor()

# Build URL for a specific game
url = extractor.get_game_data_url(
    game_id="257324",
    date="july-1-2025",
    team_slug="yankees-blue-jays",
    build_id="5ewWa01cMp6swFO15XxC9"  # Optional, will extract if not provided
)

print(url)
# Output: https://www.actionnetwork.com/_next/data/5ewWa01cMp6swFO15XxC9/mlb-game/yankees-blue-jays-score-odds-july-1-2025/257324.json?league=mlb-game&slug=yankees-blue-jays-score-odds-july-1-2025&gameId=257324
```

### 3. Run the Demo

```bash
uv run python action/examples/url_builder_demo.py
```

## 🔗 URL Pattern Analysis

Based on your research, Action Network URLs follow this pattern:

```
https://www.actionnetwork.com/_next/data/{BUILD_ID}/mlb-game/{TEAM_SLUG}-score-odds-{DATE}/{GAME_ID}.json?league=mlb-game&slug={TEAM_SLUG}-score-odds-{DATE}&gameId={GAME_ID}
```

**What changes between URLs:**
- `{BUILD_ID}`: Next.js build hash (e.g., `5ewWa01cMp6swFO15XxC9`)
- `{GAME_ID}`: Unique game identifier (e.g., `257324`, `257326`)
- `{TEAM_SLUG}`: Team names in URL format (e.g., `yankees-blue-jays`)
- `{DATE}`: Date in format like `july-1-2025`

**Examples:**
- Yankees @ Blue Jays: `https://www.actionnetwork.com/_next/data/5ewWa01cMp6swFO15XxC9/mlb-game/yankees-blue-jays-score-odds-july-1-2025/257324.json`
- Cardinals @ Pirates: `https://www.actionnetwork.com/_next/data/5ewWa01cMp6swFO15XxC9/mlb-game/cardinals-pirates-score-odds-july-1-2025/257326.json`

## 🛠️ Requirements

- **Chrome Browser**: Required for Selenium WebDriver
- **Python Packages**: `selenium`, `requests`
- **Internet Access**: To extract build IDs and fetch data

## ⚙️ How It Works

### Build ID Extraction

1. Opens Action Network's sharp report page with Chrome (headless)
2. Monitors network traffic for `_next/data` requests
3. Extracts build ID from URLs using regex patterns
4. Falls back to HTML parsing if network monitoring fails
5. Caches build IDs for 1 hour to avoid repeated extraction

### URL Construction

1. Takes game data (ID, teams, date)
2. Normalizes team names to URL slugs
3. Formats dates appropriately
4. Combines with build ID to create complete URLs

## 🎯 Use Cases

1. **Live Game Monitoring**: Get real-time game data from Action Network
2. **Historical Analysis**: Access archived game information
3. **Betting Analysis**: Compare odds and market movements
4. **Data Pipeline**: Automate data collection from Action Network

## 🔍 Demo Results

The demo script successfully:
- ✅ Extracted build ID: `5ewWa01cMp6swFO15XxC9`
- ✅ Constructed URLs for Yankees @ Blue Jays (Game 257324)
- ✅ Constructed URLs for Cardinals @ Pirates (Game 257326)  
- ✅ **API integration (15 games retrieved - 403 error resolved!)**
- ✅ **Game data fetching (668KB+ of live data per game)**

## 🎉 API Integration Success

**Problem Solved!** The 403 Forbidden errors have been resolved by implementing **browser-like headers**:

- ✅ **User-Agent**: Mimics Chrome browser requests
- ✅ **Referer Headers**: Shows requests originate from Action Network
- ✅ **Session Management**: Establishes proper session before API calls
- ✅ **Security Headers**: Includes required `Sec-Fetch-*` headers

**Result**: Now successfully retrieving 15+ games per API call with 600KB+ of live data per game.

## 🚧 Known Limitations

1. **Build ID Changes**: Build IDs change when Action Network deploys (handled automatically with caching)
2. **Rate Limiting**: Frequent requests may be blocked (use responsibly)
3. **Chrome Dependency**: Requires Chrome browser for build ID extraction

## 📈 Future Enhancements

1. **Authentication**: Add support for authenticated API requests
2. **Retry Logic**: Implement exponential backoff for failed requests
3. **Proxy Support**: Add proxy rotation for large-scale scraping
4. **Database Integration**: Store build IDs and game data in database
5. **Real-time Updates**: Monitor for build ID changes automatically

## 🔗 Related APIs

The utilities work with these Action Network endpoints:

- **Sharp Report**: `https://www.actionnetwork.com/mlb/sharp-report`
- **Scoreboard API**: `https://api.actionnetwork.com/web/v2/scoreboard/proreport/mlb`
- **Game Data**: `https://www.actionnetwork.com/_next/data/{BUILD_ID}/mlb-game/...`

---

**General Balls** 🏈 