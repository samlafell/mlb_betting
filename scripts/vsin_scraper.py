#!/usr/bin/env python3
"""
VSIN HTML Scraper

This script retrieves HTML data from the VSIN sports betting website and saves it to a file.
The script specifically extracts the main content div with betting data.
Supports multiple sports: NFL, NBA, MLB, NHL, CBB (College Basketball), CFB (College Football), etc.
"""
import os
import sys
import time
import argparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Sports URL mappings
SPORTS_URLS = {
    'nfl': 'nfl/betting-splits',
    'nba': 'nba/betting-splits', 
    'mlb': 'mlb/betting-splits',
    'nhl': 'nhl/betting-splits',
    'cbb': 'college-basketball/betting-splits',  # College Basketball
    'cfb': 'college-football/betting-splits',    # College Football
    'wnba': 'wnba/betting-splits',
    'ufc': 'ufc/betting-splits',
    'pga': 'pga/betting-splits',
    'tennis': 'tennis/betting-splits',
    'epl': 'epl/betting-splits',                 # English Premier League
    'ufl': 'ufl/betting-splits'                  # United Football League
}

# Sportsbook view parameters
SPORTSBOOK_VIEWS = {
    'circa': '?view=circa',
    'dk': '?',  # DraftKings is default view
    'fanduel': '?view=fanduel',
    'mgm': '?view=mgm',
    'caesars': '?view=caesars'
}

OUTPUT_DIR = "examples"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


def create_output_directory(directory):
    """
    Create the output directory if it doesn't exist.
    
    Args:
        directory (str): The path to the output directory
    """
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        except OSError as e:
            print(f"Error creating directory {directory}: {e}")
            sys.exit(1)


def fetch_html_content(url):
    """
    Fetch HTML content from the specified URL.
    
    Args:
        url (str): The URL to fetch HTML from
        
    Returns:
        str: The HTML content
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://data.vsin.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }
    
    try:
        print(f"Fetching HTML from {url}...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching HTML: {e}")
        sys.exit(1)


def extract_main_content(html_content):
    """
    Extract the main content div from the HTML content.
    
    Args:
        html_content (str): The HTML content to parse
        
    Returns:
        str: The HTML content of the main content div
    """
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        main_content_div = soup.find('div', {'class': 'main-content paywall-active', 'id': 'main-content'})
        
        if main_content_div:
            return str(main_content_div)
        else:
            # Fallback to look for similar elements if the exact match isn't found
            main_content_div = soup.find('div', {'id': 'main-content'})
            if main_content_div:
                return str(main_content_div)
            
            main_content_div = soup.find('div', {'class': 'main-content'})
            if main_content_div:
                return str(main_content_div)
                
            # If all else fails, look for the most likely container of betting data
            betting_table = soup.find('table', {'class': 'freezetable'})
            if betting_table:
                return str(betting_table)
            
            print("Warning: Could not find the main content div. Saving the entire HTML content.")
            return html_content
    except Exception as e:
        print(f"Error parsing HTML: {e}")
        return html_content


def build_url(sport, sportsbook='dk'):
    """
    Build the complete URL for scraping VSIN data.
    
    Args:
        sport (str): The sport to scrape (nfl, nba, mlb, etc.)
        sportsbook (str): The sportsbook view (circa, dk, fanduel, etc.)
        
    Returns:
        str: The complete URL
    """
    base_url = "https://data.vsin.com"
    sport_path = SPORTS_URLS.get(sport.lower())
    sportsbook_param = SPORTSBOOK_VIEWS.get(sportsbook.lower(), '?')
    
    if not sport_path:
        raise ValueError(f"Sport '{sport}' not supported. Available sports: {', '.join(SPORTS_URLS.keys())}")
    
    return f"{base_url}/{sport_path}/{sportsbook_param}"


def save_html_to_file(content, directory, sport, sportsbook):
    """
    Save HTML content to a file.
    
    Args:
        content (str): The HTML content to save
        directory (str): The directory to save the file to
        sport (str): The sport being scraped
        sportsbook (str): The name of the sportsbook (circa, dk, etc.)
    
    Returns:
        str: The path to the saved file
    """
    # Generate a filename with the sport, sportsbook, current date and time
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"vsin_html_{sport}_{sportsbook}_{timestamp}.html"
    
    file_path = os.path.join(directory, filename)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)
        print(f"HTML content saved to {file_path}")
        return file_path
    except IOError as e:
        print(f"Error saving HTML content: {e}")
        sys.exit(1)


def main():
    """
    Main function to run the script.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Scrape VSIN betting data from different sports and sportsbooks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Available Sports: {', '.join(SPORTS_URLS.keys())}
Available Sportsbooks: {', '.join(SPORTSBOOK_VIEWS.keys())}

Examples:
  python vsin_scraper.py nfl circa     # NFL data from Circa
  python vsin_scraper.py nba dk        # NBA data from DraftKings  
  python vsin_scraper.py mlb fanduel   # MLB data from FanDuel
  python vsin_scraper.py cbb circa     # College Basketball from Circa
        """
    )
    
    parser.add_argument('sport', 
                        choices=list(SPORTS_URLS.keys()),
                        help='The sport to scrape')
    parser.add_argument('sportsbook', 
                        choices=list(SPORTSBOOK_VIEWS.keys()),
                        help='The sportsbook to scrape from')
    
    args = parser.parse_args()
    
    # Create the output directory if it doesn't exist
    create_output_directory(OUTPUT_DIR)
    
    # Build the URL
    try:
        url = build_url(args.sport, args.sportsbook)
        print(f"Scraping {args.sport.upper()} data from {args.sportsbook.upper()}")
        print(f"URL: {url}")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Fetch the HTML content
    html_content = fetch_html_content(url)
    
    # Extract the main content div
    main_content = extract_main_content(html_content)
    
    # Save the main content to a file
    save_html_to_file(main_content, OUTPUT_DIR, args.sport, args.sportsbook)
    
    print("Done! The HTML content has been successfully retrieved and saved.")


if __name__ == "__main__":
    main() 