"""
Fixture data for API responses.

Contains realistic API response samples for testing.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List


class ActionNetworkFixtures:
    """Fixtures for Action Network API responses."""
    
    @staticmethod
    def game_response() -> Dict[str, Any]:
        """Sample game response from Action Network."""
        return {
            "id": "an_test_game_123",
            "status": "scheduled",
            "start_time": "2024-07-30T19:00:00Z",
            "league": "mlb",
            "season": 2024,
            "teams": [
                {
                    "id": "team_home_123",
                    "display_name": "New York Yankees",
                    "abbreviation": "NYY",
                    "location": "home",
                    "conference": "AL",
                    "division": "East"
                },
                {
                    "id": "team_away_456",
                    "display_name": "Boston Red Sox", 
                    "abbreviation": "BOS",
                    "location": "away",
                    "conference": "AL",
                    "division": "East"
                }
            ],
            "venue": {
                "id": "venue_123",
                "name": "Yankee Stadium",
                "location": "Bronx, NY"
            },
            "weather": {
                "temperature": 78,
                "condition": "Clear",
                "wind_speed": 5,
                "wind_direction": "NW"
            }
        }
    
    @staticmethod
    def odds_response() -> List[Dict[str, Any]]:
        """Sample odds response from Action Network."""
        return [
            {
                "sportsbook_id": "draftkings",
                "sportsbook_name": "DraftKings",
                "market_type": "moneyline",
                "outcomes": [
                    {"name": "home", "odds": -150, "line": None},
                    {"name": "away", "odds": 130, "line": None}
                ],
                "updated_at": "2024-07-30T17:30:00Z",
                "movement": {
                    "direction": "home",
                    "previous_home_odds": -140,
                    "previous_away_odds": 120
                }
            },
            {
                "sportsbook_id": "fanduel",
                "sportsbook_name": "FanDuel",
                "market_type": "spread",
                "outcomes": [
                    {"name": "home", "odds": -110, "line": -1.5},
                    {"name": "away", "odds": -110, "line": 1.5}
                ],
                "updated_at": "2024-07-30T17:30:00Z",
                "movement": {
                    "direction": "away",
                    "previous_line": -2.0
                }
            },
            {
                "sportsbook_id": "betmgm",
                "sportsbook_name": "BetMGM",
                "market_type": "total",
                "outcomes": [
                    {"name": "over", "odds": -110, "line": 8.5},
                    {"name": "under", "odds": -110, "line": 8.5}
                ],
                "updated_at": "2024-07-30T17:30:00Z",
                "movement": {
                    "direction": "under",
                    "previous_line": 9.0
                }
            }
        ]
    
    @staticmethod
    def historical_odds() -> List[Dict[str, Any]]:
        """Sample historical odds data."""
        base_time = datetime.utcnow()
        history = []
        
        for i in range(24):  # 24 hours of data
            timestamp = base_time - timedelta(hours=i)
            
            # Simulate line movement over time
            home_odds = -150 + (i * 2)  # Moving toward home
            away_odds = 130 - (i * 2)
            spread = -1.5 - (i * 0.1)
            total = 8.5 + (i * 0.1)
            
            history.append({
                "timestamp": timestamp.isoformat(),
                "sportsbook": "draftkings",
                "market_type": "moneyline",
                "home_odds": home_odds,
                "away_odds": away_odds,
                "volume": "high" if i % 3 == 0 else "normal"
            })
            
            history.append({
                "timestamp": timestamp.isoformat(),
                "sportsbook": "fanduel",
                "market_type": "spread",
                "line": spread,
                "home_odds": -110,
                "away_odds": -110,
                "volume": "normal"
            })
            
            history.append({
                "timestamp": timestamp.isoformat(),
                "sportsbook": "betmgm",
                "market_type": "total",
                "line": total,
                "over_odds": -110,
                "under_odds": -110,
                "volume": "high" if i % 4 == 0 else "normal"
            })
        
        return sorted(history, key=lambda x: x["timestamp"])
    
    @staticmethod
    def complete_game_data() -> Dict[str, Any]:
        """Complete game data with odds and history."""
        return {
            "game": ActionNetworkFixtures.game_response(),
            "current_odds": ActionNetworkFixtures.odds_response(),
            "historical_odds": ActionNetworkFixtures.historical_odds(),
            "sharp_action": {
                "moneyline_sharp_percentage": 65,
                "spread_sharp_percentage": 72,
                "total_sharp_percentage": 58,
                "reverse_line_movement": True,
                "steam_moves": ["spread_away", "total_under"]
            }
        }


class SBDFixtures:
    """Fixtures for SportsBettingDime API responses."""
    
    @staticmethod
    def wordpress_post() -> Dict[str, Any]:
        """Sample WordPress post from SBD."""
        return {
            "id": 12345,
            "date": "2024-07-30T16:00:00",
            "date_gmt": "2024-07-30T20:00:00",
            "guid": {"rendered": "https://sportsbettingdime.com/?p=12345"},
            "modified": "2024-07-30T17:30:00",
            "slug": "yankees-red-sox-betting-preview",
            "status": "publish",
            "type": "post",
            "title": {"rendered": "Yankees vs Red Sox Betting Preview"},
            "content": {
                "rendered": "<p>Latest betting odds and analysis for tonight's Yankees vs Red Sox matchup...</p>"
            },
            "excerpt": {"rendered": "Betting preview for Yankees vs Red Sox"},
            "author": 5,
            "categories": [10, 15],
            "tags": [25, 30, 35],
            "meta": {
                "game_id": "sbd_game_123",
                "home_team": "Yankees",
                "away_team": "Red Sox",
                "game_date": "2024-07-30",
                "sportsbooks": ["draftkings", "fanduel", "betmgm", "caesars"],
                "odds_data": {
                    "moneyline": {
                        "home": -155,
                        "away": 135,
                        "movement": "toward_home"
                    },
                    "spread": {
                        "line": -1.5,
                        "home_odds": -110,
                        "away_odds": -110,
                        "movement": "line_down"
                    },
                    "total": {
                        "line": 8.5,
                        "over_odds": -110,
                        "under_odds": -110,
                        "movement": "line_up"
                    }
                },
                "betting_trends": {
                    "public_bets_home": 45,
                    "public_money_home": 38,
                    "sharp_action": "away_spread"
                }
            }
        }
    
    @staticmethod
    def posts_response() -> Dict[str, Any]:
        """Sample posts endpoint response."""
        return {
            "posts": [SBDFixtures.wordpress_post()],
            "total": 1,
            "total_pages": 1,
            "per_page": 10,
            "page": 1
        }


class VSINFixtures:
    """Fixtures for VSIN scraping responses."""
    
    @staticmethod
    def sharp_report_html() -> str:
        """Sample VSIN sharp report HTML."""
        return """
        <!DOCTYPE html>
        <html>
        <head><title>VSIN Sharp Report</title></head>
        <body>
            <div class="sharp-report-container">
                <div class="report-header">
                    <h1>Sharp Report - July 30, 2024</h1>
                </div>
                <div class="games-list">
                    <div class="game-row" data-game-id="vsin_123">
                        <div class="teams">NYY @ BOS</div>
                        <div class="game-time">7:00 PM ET</div>
                        <div class="sharp-indicators">
                            <span class="sharp-action">75% Sharp Money on BOS +1.5</span>
                            <span class="line-movement">Line moved from +2 to +1.5</span>
                            <span class="steam-move">STEAM MOVE</span>
                            <span class="reverse-line-movement">RLM</span>
                        </div>
                        <div class="betting-percentages">
                            <span class="public-bets">Public: 65% NYY</span>
                            <span class="sharp-bets">Sharp: 75% BOS</span>
                        </div>
                    </div>
                    <div class="game-row" data-game-id="vsin_456">
                        <div class="teams">LAD @ SF</div>
                        <div class="game-time">10:00 PM ET</div>
                        <div class="sharp-indicators">
                            <span class="sharp-action">Sharp money on Under 9</span>
                            <span class="line-movement">Total dropped from 9.5 to 9</span>
                        </div>
                        <div class="betting-percentages">
                            <span class="public-bets">Public: 58% Over</span>
                            <span class="sharp-bets">Sharp: 72% Under</span>
                        </div>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
    
    @staticmethod
    def parsed_reports() -> List[Dict[str, Any]]:
        """Sample parsed VSIN reports."""
        return [
            {
                "game_id": "vsin_123",
                "teams": "NYY @ BOS",
                "game_time": "7:00 PM ET",
                "sharp_action": "75% Sharp Money on BOS +1.5",
                "line_movement": "Line moved from +2 to +1.5",
                "indicators": ["STEAM MOVE", "RLM"],
                "public_percentage": 65,
                "public_side": "NYY",
                "sharp_percentage": 75,
                "sharp_side": "BOS",
                "market": "spread"
            },
            {
                "game_id": "vsin_456",
                "teams": "LAD @ SF", 
                "game_time": "10:00 PM ET",
                "sharp_action": "Sharp money on Under 9",
                "line_movement": "Total dropped from 9.5 to 9",
                "indicators": [],
                "public_percentage": 58,
                "public_side": "Over",
                "sharp_percentage": 72,
                "sharp_side": "Under",
                "market": "total"
            }
        ]


class MLBStatsFixtures:
    """Fixtures for MLB Stats API responses."""
    
    @staticmethod
    def schedule_response() -> Dict[str, Any]:
        """Sample MLB schedule response."""
        return {
            "copyright": "Copyright 2024 MLB Advanced Media, L.P.",
            "totalItems": 15,
            "totalEvents": 0,
            "totalGames": 15,
            "totalGamesInProgress": 0,
            "dates": [
                {
                    "date": "2024-07-30",
                    "totalItems": 15,
                    "totalEvents": 0,
                    "totalGames": 15,
                    "totalGamesInProgress": 0,
                    "games": [
                        {
                            "gamePk": 746789,
                            "link": "/api/v1/games/746789",
                            "gameType": "R",
                            "season": "2024",
                            "gameDate": "2024-07-30T23:00:00Z",
                            "status": {
                                "abstractGameState": "Preview",
                                "codedGameState": "S",
                                "detailedState": "Scheduled",
                                "statusCode": "S"
                            },
                            "teams": {
                                "away": {
                                    "leagueRecord": {"wins": 68, "losses": 42, "pct": ".618"},
                                    "score": 0,
                                    "team": {
                                        "id": 111,
                                        "name": "Boston Red Sox",
                                        "link": "/api/v1/teams/111"
                                    },
                                    "isWinner": False,
                                    "splitSquad": False,
                                    "seriesNumber": 45
                                },
                                "home": {
                                    "leagueRecord": {"wins": 62, "losses": 48, "pct": ".564"},
                                    "score": 0,
                                    "team": {
                                        "id": 147,
                                        "name": "New York Yankees",
                                        "link": "/api/v1/teams/147"
                                    },
                                    "isWinner": False,
                                    "splitSquad": False,
                                    "seriesNumber": 45
                                }
                            },
                            "venue": {
                                "id": 3313,
                                "name": "Yankee Stadium",
                                "link": "/api/v1/venues/3313"
                            },
                            "content": {"link": "/api/v1/game/746789/content"},
                            "gameNumber": 1,
                            "publicFacing": True,
                            "doubleHeader": "N",
                            "gamedayType": "P",
                            "tiebreaker": "N",
                            "calendarEventID": "14-746789-2024-07-30",
                            "seasonDisplay": "2024",
                            "dayNight": "night",
                            "scheduledInnings": 9,
                            "reverseHomeAwayStatus": False,
                            "inningBreakLength": 120,
                            "gamesInSeries": 3,
                            "seriesGameNumber": 2,
                            "seriesDescription": "Regular Season",
                            "recordSource": "S",
                            "ifNecessary": "N",
                            "ifNecessaryDescription": "Normal Game"
                        }
                    ]
                }
            ]
        }


def get_all_fixtures() -> Dict[str, Any]:
    """Get all fixture data organized by source."""
    return {
        "action_network": {
            "game": ActionNetworkFixtures.game_response(),
            "odds": ActionNetworkFixtures.odds_response(),
            "historical_odds": ActionNetworkFixtures.historical_odds(),
            "complete_game": ActionNetworkFixtures.complete_game_data()
        },
        "sbd": {
            "post": SBDFixtures.wordpress_post(),
            "posts_response": SBDFixtures.posts_response()
        },
        "vsin": {
            "html": VSINFixtures.sharp_report_html(),
            "parsed_reports": VSINFixtures.parsed_reports()
        },
        "mlb_stats": {
            "schedule": MLBStatsFixtures.schedule_response()
        }
    }