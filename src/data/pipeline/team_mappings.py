#!/usr/bin/env python3
"""
Team Mapping Configuration

Centralized team ID mappings for different data sources.
Externalized from processors to improve maintainability and flexibility.
"""

from typing import Dict

# Action Network team ID mapping
# Discovered from data analysis and Action Network API documentation
ACTION_NETWORK_TEAM_MAPPING: Dict[int, str] = {
    # American League East
    189: "NYY",  # New York Yankees
    194: "BOS",  # Boston Red Sox
    195: "TOR",  # Toronto Blue Jays
    196: "TB",   # Tampa Bay Rays
    197: "BAL",  # Baltimore Orioles
    
    # American League Central
    200: "CWS",  # Chicago White Sox
    201: "CLE",  # Cleveland Guardians
    205: "DET",  # Detroit Tigers
    203: "KC",   # Kansas City Royals
    207: "MIN",  # Minnesota Twins
    
    # American League West
    208: "HOU",  # Houston Astros
    210: "LAA",  # Los Angeles Angels
    211: "OAK",  # Oakland Athletics
    209: "SEA",  # Seattle Mariners
    206: "TEX",  # Texas Rangers
    
    # National League East
    198: "ATL",  # Atlanta Braves
    199: "MIA",  # Miami Marlins
    204: "NYM",  # New York Mets
    212: "PHI",  # Philadelphia Phillies
    216: "WSH",  # Washington Nationals
    
    # National League Central
    213: "CHC",  # Chicago Cubs
    202: "CIN",  # Cincinnati Reds
    214: "MIL",  # Milwaukee Brewers
    215: "PIT",  # Pittsburgh Pirates
    217: "STL",  # St. Louis Cardinals
    
    # National League West
    218: "ARI",  # Arizona Diamondbacks
    219: "COL",  # Colorado Rockies
    220: "LAD",  # Los Angeles Dodgers
    221: "SD",   # San Diego Padres
    222: "SF",   # San Francisco Giants
}

# Other data source mappings can be added here
VSIN_TEAM_MAPPING: Dict[str, str] = {
    # Add VSIN-specific mappings as needed
}

SBD_TEAM_MAPPING: Dict[str, str] = {
    # Add SBD-specific mappings as needed
}

def get_team_mapping(source: str) -> Dict:
    """
    Get team mapping for a specific data source.
    
    Args:
        source: Data source name ('action_network', 'vsin', 'sbd', etc.)
        
    Returns:
        Dictionary mapping source-specific team IDs to standard abbreviations
    """
    mappings = {
        'action_network': ACTION_NETWORK_TEAM_MAPPING,
        'vsin': VSIN_TEAM_MAPPING,
        'sbd': SBD_TEAM_MAPPING,
    }
    
    return mappings.get(source.lower(), {})