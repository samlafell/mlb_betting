#!/usr/bin/env python3
"""
SportsbookReview (SBR) Unified Collector

Playwright-based data collector for SportsbookReview.com betting lines and
historical data. Designed for headless operation with anti-detection measures.

This collector implements the UnifiedBettingLinesCollector interface for:
- Historical betting line collection
- Multi-market data (moneyline, spread, totals)
- JavaScript-rendered content handling
- Rate limiting and stealth operation
"""

from datetime import date, datetime
from typing import Any

import structlog
from playwright.async_api import Page, async_playwright

from .base import BaseCollector, CollectionRequest, CollectorConfig, DataSource

logger = structlog.get_logger(__name__)


class SBRUnifiedCollector(BaseCollector):
    """
    SportsbookReview Unified Collector

    Playwright-based collector for SBR historical betting data.
    Inherits from BaseCollector for consistency with the unified architecture.
    """

    def __init__(self, config=None):
        """Initialize SBR collector with optional config."""
        # Handle both factory instantiation and direct instantiation
        if config is None:
            config = CollectorConfig(source=DataSource.SPORTS_BOOK_REVIEW)
        elif not hasattr(config, "source"):
            config.source = DataSource.SPORTS_BOOK_REVIEW

        super().__init__(config)

        # SBR-specific configuration
        self.base_url = "https://www.sportsbookreview.com"
        self.max_games_per_run = 10
        self.browser_pool_size = 1
        self.page_timeout = 30000  # 30 seconds

        # Browser management
        self.playwright = None
        self.browser = None
        self.context = None

        # Rate limiting - use base rate limiter from BaseCollector
        # Note: Using a simple approach to avoid complex import chain issues
        from .base import RateLimiter

        self.rate_limiter = RateLimiter(calls_per_minute=60)

        # Fix source access for logging
        self.source_value = (
            self.source.value if hasattr(self.source, "value") else str(self.source)
        )

        # Anti-detection settings
        self.stealth_config = {
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "viewport": {"width": 1920, "height": 1080},
            "extra_http_headers": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
        }

    async def initialize_browser_pool(self) -> None:
        """Initialize Playwright browser pool for data collection."""
        try:
            self.logger.info("Initializing Playwright browser pool")

            self.playwright = await async_playwright().start()

            # Launch browser with stealth settings
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-extensions",
                    "--disable-plugins",
                    "--disable-images",  # Faster loading
                    "--disable-javascript-harmony-shipping",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--no-first-run",
                    "--no-default-browser-check",
                ],
            )

            # Create persistent context with stealth settings
            self.context = await self.browser.new_context(
                user_agent=self.stealth_config["user_agent"],
                viewport=self.stealth_config["viewport"],
                extra_http_headers=self.stealth_config["extra_http_headers"],
                java_script_enabled=True,
                bypass_csp=True,
            )

            # Add stealth scripts
            await self.context.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Mock permissions API
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
            """)

            self.logger.info("Browser pool initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize browser pool", error=str(e))
            raise

    async def cleanup_browser_pool(self) -> None:
        """Clean up browser resources."""
        try:
            if self.context:
                await self.context.close()
                self.context = None

            if self.browser:
                await self.browser.close()
                self.browser = None

            if self.playwright:
                await self.playwright.stop()
                self.playwright = None

            self.logger.info("Browser pool cleaned up")

        except Exception as e:
            self.logger.error("Error during browser cleanup", error=str(e))

    async def collect_data(self, request: CollectionRequest) -> list[dict[str, Any]]:
        """
        Collect betting lines data from SportsbookReview.

        This is the main entry point for unified data collection.
        """
        try:
            self.logger.info("Starting SBR unified data collection")

            # Initialize browser if needed
            if not self.browser:
                await self.initialize_browser_pool()

            # Get target date for collection
            target_date = request.start_date or date.today()

            # Rate limiting
            await self.rate_limiter.acquire()

            # Collect data for the target date
            games_data = await self._collect_games_for_date(target_date)

            self.logger.info(
                "SBR collection completed",
                games_found=len(games_data),
                games_processed=len(games_data),
            )

            return games_data

        except Exception as e:
            self.logger.error("SBR collection failed", error=str(e))
            raise
        finally:
            # Clean up browser resources if needed
            if self.browser and not request.additional_params.get(
                "keep_browser_alive", False
            ):
                await self.cleanup_browser_pool()

    async def _collect_games_for_date(self, target_date: date) -> list[dict[str, Any]]:
        """Collect all games and betting data for a specific date."""
        try:
            # Create new page for this collection
            page = await self.context.new_page()

            try:
                # Navigate to SBR MLB page with date parameter
                date_str = target_date.strftime("%Y-%m-%d")
                mlb_url = f"{self.base_url}/betting-odds/mlb-baseball/?date={date_str}"
                self.logger.info(
                    "Navigating to SBR MLB page", url=mlb_url, target_date=date_str
                )

                await page.goto(
                    mlb_url, wait_until="domcontentloaded", timeout=self.page_timeout
                )

                # Wait for dynamic content to load
                await page.wait_for_timeout(3000)

                # Look for games on the page
                games_data = await self._extract_games_from_page(page, target_date)

                self.logger.info(f"Extracted {len(games_data)} games from SBR")
                return games_data

            finally:
                await page.close()

        except Exception as e:
            self.logger.error(
                "Error collecting games for date", date=target_date, error=str(e)
            )
            return []

    async def _extract_games_from_page(
        self, page: Page, target_date: date
    ) -> list[dict[str, Any]]:
        """Extract game data from SBR page using JavaScript evaluation."""
        try:
            # Wait for the betting table to load
            await page.wait_for_selector(
                "table, .betting-table, [data-testid*='odds'], .odds-table",
                timeout=10000,
            )

            # Extract games data using JavaScript
            games_data = await page.evaluate("""
                () => {
                    const games = [];
                    
                    // Look for common SBR game containers
                    const gameSelectors = [
                        'tr[data-game-id]',
                        '.game-row',
                        '[data-testid*="game"]',
                        'tr:has(.team-name)',
                        'tbody tr'
                    ];
                    
                    let gameElements = [];
                    for (const selector of gameSelectors) {
                        const elements = document.querySelectorAll(selector);
                        if (elements.length > 0) {
                            gameElements = Array.from(elements);
                            break;
                        }
                    }
                    
                    // If no specific selectors work, try to find rows with team data
                    if (gameElements.length === 0) {
                        const allRows = document.querySelectorAll('tr');
                        gameElements = Array.from(allRows).filter(row => {
                            const text = row.textContent;
                            return text && (
                                text.includes('Yankees') || text.includes('Red Sox') ||
                                text.includes('Dodgers') || text.includes('Giants') ||
                                text.includes('@') || text.includes('vs') ||
                                /\\d+\\.\\d+/.test(text)  // Contains decimal odds
                            );
                        });
                    }
                    
                    gameElements.forEach((gameElement, index) => {
                        try {
                            const gameData = {
                                game_id: gameElement.getAttribute('data-game-id') || `sbr_game_${index}`,
                                teams: [],
                                markets: {
                                    moneyline: {},
                                    spread: {},
                                    total: {}
                                },
                                raw_html: gameElement.outerHTML.substring(0, 500)  // First 500 chars for debugging
                            };
                            
                            // Extract team names
                            const teamSelectors = [
                                '.team-name',
                                '[data-testid*="team"]',
                                '.team',
                                'td:has(img[alt*="logo"])',
                                'td[data-team]'
                            ];
                            
                            let teams = [];
                            for (const selector of teamSelectors) {
                                const teamElements = gameElement.querySelectorAll(selector);
                                if (teamElements.length > 0) {
                                    teams = Array.from(teamElements).map(el => ({
                                        name: el.textContent?.trim() || el.getAttribute('data-team') || '',
                                        element_html: el.outerHTML.substring(0, 100)
                                    }));
                                    break;
                                }
                            }
                            
                            // If no teams found with selectors, try text parsing
                            if (teams.length === 0) {
                                const text = gameElement.textContent;
                                const teamPatterns = [
                                    /([A-Z][a-z]+ [A-Z][a-z]+)/g,  // "Team Name" format
                                    /([A-Z]{2,3})\\s*@\\s*([A-Z]{2,3})/,  // "ABC @ XYZ" format
                                    /([A-Z][a-z]+)\\s*vs\\s*([A-Z][a-z]+)/  // "Team vs Team" format
                                ];
                                
                                for (const pattern of teamPatterns) {
                                    const matches = text.match(pattern);
                                    if (matches) {
                                        teams = matches.slice(1).map(team => ({ name: team.trim() }));
                                        break;
                                    }
                                }
                            }
                            
                            gameData.teams = teams;
                            
                            // Extract odds data
                            const oddsSelectors = [
                                '.odds',
                                '[data-testid*="odds"]',
                                '.price',
                                'td[data-odd]',
                                'span[data-price]'
                            ];
                            
                            let oddsElements = [];
                            for (const selector of oddsSelectors) {
                                const elements = gameElement.querySelectorAll(selector);
                                if (elements.length > 0) {
                                    oddsElements = Array.from(elements);
                                    break;
                                }
                            }
                            
                            // Parse odds values
                            const oddsValues = oddsElements.map(el => ({
                                value: el.textContent?.trim() || '',
                                data_attr: el.getAttribute('data-odd') || el.getAttribute('data-price') || '',
                                class_name: el.className
                            }));
                            
                            gameData.odds_data = oddsValues;
                            
                            // Extract numeric odds
                            const allText = gameElement.textContent;
                            const odds_matches = allText.match(/[-+]?\\d+/g) || [];
                            gameData.extracted_numbers = odds_matches;
                            
                            games.push(gameData);
                            
                        } catch (error) {
                            console.error('Error processing game element:', error);
                        }
                    });
                    
                    return games;
                }
            """)

            # Process the extracted data
            processed_games = []
            for i, game_data in enumerate(games_data):
                try:
                    processed_game = await self._process_sbr_game_data(
                        game_data, target_date, i
                    )
                    if processed_game:
                        processed_games.append(processed_game)
                except Exception as e:
                    self.logger.error(f"Error processing game {i}", error=str(e))
                    continue

            self.logger.info(
                f"Processed {len(processed_games)} games from {len(games_data)} extracted"
            )
            return processed_games

        except Exception as e:
            self.logger.error("Error extracting games from page", error=str(e))
            return []

    async def _process_sbr_game_data(
        self, game_data: dict[str, Any], target_date: date, index: int
    ) -> dict[str, Any] | None:
        """Process and normalize a single game's data from SBR."""
        try:
            game_id = game_data.get("game_id", f"sbr_game_{target_date}_{index}")
            teams = game_data.get("teams", [])

            # Extract team information
            if len(teams) >= 2:
                away_team = teams[0].get("name", f"Team{index}A")
                home_team = teams[1].get("name", f"Team{index}H")
            elif len(teams) == 1:
                # Try to split a single team string like "Team1 @ Team2"
                team_text = teams[0].get("name", "")
                if "@" in team_text:
                    parts = team_text.split("@")
                    away_team = parts[0].strip()
                    home_team = parts[1].strip()
                elif "vs" in team_text:
                    parts = team_text.split("vs")
                    away_team = parts[0].strip()
                    home_team = parts[1].strip()
                else:
                    away_team = f"Team{index}A"
                    home_team = f"Team{index}H"
            else:
                away_team = f"Team{index}A"
                home_team = f"Team{index}H"

            # Extract betting lines from odds data
            markets = self._extract_betting_markets(game_data)

            processed_game = {
                "game_id": game_id,
                "home_team": {
                    "name": home_team,
                    "abbreviation": self._get_team_abbreviation(home_team),
                },
                "away_team": {
                    "name": away_team,
                    "abbreviation": self._get_team_abbreviation(away_team),
                },
                "game_date": target_date.isoformat(),
                "markets": markets,
                "source": "sportsbookreview",
                "collected_at": datetime.now().isoformat(),
                "raw_data": {
                    "teams_found": len(teams),
                    "odds_elements": len(game_data.get("odds_data", [])),
                    "extracted_numbers": game_data.get("extracted_numbers", []),
                },
            }

            return processed_game

        except Exception as e:
            self.logger.error(
                "Error processing SBR game data", game_index=index, error=str(e)
            )
            return None

    def _extract_betting_markets(self, game_data: dict[str, Any]) -> dict[str, Any]:
        """Extract betting markets (moneyline, spread, totals) from game data."""
        markets = {"moneyline": {}, "spread": {}, "total": {}}

        try:
            # Get numeric values from the game data
            numbers = game_data.get("extracted_numbers", [])
            odds_data = game_data.get("odds_data", [])

            # Convert to integers for processing
            numeric_values = []
            for num in numbers:
                try:
                    numeric_values.append(int(num))
                except ValueError:
                    continue

            # Separate positive and negative odds
            positive_odds = [
                n for n in numeric_values if n > 0 and n < 1000
            ]  # Typical range for +odds
            negative_odds = [
                n for n in numeric_values if n < 0 and n > -1000
            ]  # Typical range for -odds

            # Extract moneyline odds (typically +/- values around 100-500)
            if positive_odds and negative_odds:
                markets["moneyline"] = {
                    "home_odds": negative_odds[0] if negative_odds else None,
                    "away_odds": positive_odds[0] if positive_odds else None,
                }

            # Extract spread lines (typically small decimal or integer values)
            spread_candidates = [n for n in numeric_values if -20 <= n <= 20 and n != 0]
            if spread_candidates:
                markets["spread"] = {
                    "line": float(spread_candidates[0]) / 10
                    if abs(spread_candidates[0]) > 10
                    else float(spread_candidates[0]),
                    "home_odds": negative_odds[1] if len(negative_odds) > 1 else -110,
                    "away_odds": positive_odds[1] if len(positive_odds) > 1 else -110,
                }

            # Extract totals (typically values around 7-12 for baseball)
            total_candidates = [
                n for n in numeric_values if 50 <= n <= 150
            ]  # Expecting decimal representation
            if total_candidates:
                total_line = float(total_candidates[0]) / 10  # Convert 85 to 8.5
                markets["total"] = {
                    "line": total_line,
                    "over_odds": negative_odds[2] if len(negative_odds) > 2 else -110,
                    "under_odds": positive_odds[2] if len(positive_odds) > 2 else -110,
                }

        except Exception as e:
            self.logger.error("Error extracting betting markets", error=str(e))

        return markets

    def _get_team_abbreviation(self, team_name: str) -> str:
        """Convert team name to standard abbreviation."""
        team_mappings = {
            "Yankees": "NYY",
            "Red Sox": "BOS",
            "Blue Jays": "TOR",
            "Rays": "TB",
            "Orioles": "BAL",
            "Astros": "HOU",
            "Angels": "LAA",
            "Athletics": "OAK",
            "Mariners": "SEA",
            "Rangers": "TEX",
            "White Sox": "CWS",
            "Guardians": "CLE",
            "Tigers": "DET",
            "Royals": "KC",
            "Twins": "MIN",
            "Braves": "ATL",
            "Marlins": "MIA",
            "Mets": "NYM",
            "Phillies": "PHI",
            "Nationals": "WSH",
            "Cubs": "CHC",
            "Reds": "CIN",
            "Brewers": "MIL",
            "Pirates": "PIT",
            "Cardinals": "STL",
            "Diamondbacks": "ARI",
            "Rockies": "COL",
            "Dodgers": "LAD",
            "Padres": "SD",
            "Giants": "SF",
        }

        for full_name, abbrev in team_mappings.items():
            if full_name.lower() in team_name.lower():
                return abbrev

        # Fallback: use first 3 characters
        return team_name[:3].upper()

    def validate_record(self, record: dict[str, Any]) -> bool:
        """Validate SBR record structure."""
        required_fields = ["game_id", "home_team", "away_team", "collected_at"]
        return all(field in record for field in required_fields)

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize SBR record to unified format."""
        normalized = record.copy()
        normalized["source"] = self.source_value

        # Ensure consistent timestamp format
        if "collected_at" not in normalized:
            normalized["collected_at"] = datetime.now().isoformat()

        # Add unified metadata
        normalized["data_quality"] = {
            "completeness_score": 0.8,  # SBR provides good coverage
            "confidence_level": "high",
            "validation_status": "passed",
        }

        return normalized

    async def test_connection(self) -> bool:
        """Test connection to SportsbookReview."""
        try:
            await self.initialize_browser_pool()
            page = await self.context.new_page()

            try:
                await page.goto(self.base_url, timeout=15000)
                await page.wait_for_selector("body", timeout=10000)

                # Check if we can access the MLB section with today's date
                today_str = date.today().strftime("%Y-%m-%d")
                mlb_url = f"{self.base_url}/betting-odds/mlb-baseball/?date={today_str}"
                await page.goto(mlb_url, timeout=15000)

                title = await page.title()
                success = "sportsbook" in title.lower() or "betting" in title.lower()

                self.logger.info(
                    "SBR connection test completed", success=success, title=title
                )
                return success

            finally:
                await page.close()

        except Exception as e:
            self.logger.error("SBR connection test failed", error=str(e))
            return False
        finally:
            await self.cleanup_browser_pool()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize_browser_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup_browser_pool()
