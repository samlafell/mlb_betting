"""
Web scrapers for the MLB Sharp Betting system.

This module provides scrapers for various betting data sources including
VSIN, SportsBettingDime, and Pinnacle.
"""

from mlb_sharp_betting.scrapers.base import BaseScraper, ScrapingResult
from mlb_sharp_betting.scrapers.vsin import VSINScraper
from mlb_sharp_betting.scrapers.sbd import SBDScraper
from mlb_sharp_betting.scrapers.pinnacle import PinnacleScraper

__all__ = [
    "BaseScraper",
    "ScrapingResult",
    "VSINScraper", 
    "SBDScraper",
    "PinnacleScraper",
] 