#!/usr/bin/env python3
"""
Debug script to examine script tags in July 7th pages.
"""

import asyncio
import aiohttp
import re
from bs4 import BeautifulSoup


async def debug_script_content():
    """Debug the script content in July 7th pages."""
    print("=== DEBUGGING SCRIPT CONTENT ===")
    
    url = 'https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date=2025-07-07'
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html_content = await response.text()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            script_tags = soup.find_all('script')
            
            print(f"Found {len(script_tags)} script tags")
            
            for i, script in enumerate(script_tags):
                script_content = script.string
                if script_content and len(script_content) > 100:
                    print(f"\n--- Script {i+1} (length: {len(script_content)}) ---")
                    
                    # Check for various patterns
                    patterns = {
                        '"props":': 'JSON props data',
                        'window.APP_STATE': 'APP_STATE data',
                        'window.__NEXT_DATA__': 'Next.js data',
                        '"pageProps"': 'Page props data',
                        '"oddsTables"': 'Odds tables data',
                        '"gameRows"': 'Game rows data',
                        '"games"': 'Games data',
                        'mlb': 'MLB data',
                        'baseball': 'Baseball data'
                    }
                    
                    found_patterns = []
                    for pattern, description in patterns.items():
                        if pattern in script_content:
                            found_patterns.append(description)
                    
                    if found_patterns:
                        print(f"Found patterns: {', '.join(found_patterns)}")
                        
                        # Show a sample of the content
                        print(f"Sample content: {script_content[:500]}...")
                        
                        # If it looks like JSON, try to extract it
                        if '"props":' in script_content:
                            print("Attempting to extract JSON props data...")
                            json_pattern = r'{"props":(.*?),"page"'
                            match = re.search(json_pattern, script_content, re.DOTALL)
                            if match:
                                print("✅ Found JSON props pattern")
                                json_str = '{"props":' + match.group(1) + '}'
                                print(f"JSON length: {len(json_str)}")
                                
                                # Try to parse
                                try:
                                    import json
                                    data = json.loads(json_str)
                                    print("✅ Successfully parsed JSON")
                                    
                                    # Check structure
                                    if 'pageProps' in data['props']:
                                        print("✅ Found pageProps")
                                        if 'oddsTables' in data['props']['pageProps']:
                                            print("✅ Found oddsTables")
                                        else:
                                            print("❌ No oddsTables in pageProps")
                                            print(f"pageProps keys: {list(data['props']['pageProps'].keys())}")
                                    else:
                                        print("❌ No pageProps in props")
                                        print(f"props keys: {list(data['props'].keys())}")
                                        
                                except json.JSONDecodeError as e:
                                    print(f"❌ JSON parsing failed: {e}")
                            else:
                                print("❌ No JSON props pattern match")
                        
                        # Check for Next.js data
                        if 'window.__NEXT_DATA__' in script_content:
                            print("Attempting to extract Next.js data...")
                            try:
                                start = script_content.find('window.__NEXT_DATA__ = ')
                                if start != -1:
                                    start += len('window.__NEXT_DATA__ = ')
                                    end = script_content.find('</script>', start)
                                    if end != -1:
                                        json_str = script_content[start:end].strip()
                                        if json_str.endswith(';'):
                                            json_str = json_str[:-1]
                                        
                                        try:
                                            import json
                                            data = json.loads(json_str)
                                            print("✅ Successfully parsed Next.js data")
                                            
                                            # Check structure
                                            if 'props' in data:
                                                print("✅ Found props in Next.js data")
                                                if 'pageProps' in data['props']:
                                                    print("✅ Found pageProps in Next.js data")
                                                    if 'oddsTables' in data['props']['pageProps']:
                                                        print("✅ Found oddsTables in Next.js data")
                                                    else:
                                                        print("❌ No oddsTables in Next.js pageProps")
                                                        print(f"Next.js pageProps keys: {list(data['props']['pageProps'].keys())}")
                                                else:
                                                    print("❌ No pageProps in Next.js props")
                                            else:
                                                print("❌ No props in Next.js data")
                                                
                                        except json.JSONDecodeError as e:
                                            print(f"❌ Next.js JSON parsing failed: {e}")
                            except Exception as e:
                                print(f"❌ Next.js extraction failed: {e}")
                    else:
                        print("No relevant patterns found")
                        
                        # Show first 200 chars anyway
                        print(f"First 200 chars: {script_content[:200]}...")


async def main():
    """Main function."""
    print("Debug Script Content in July 7th Pages")
    print("=" * 50)
    
    await debug_script_content()
    
    print("\n✅ Debug completed!")


if __name__ == "__main__":
    asyncio.run(main()) 