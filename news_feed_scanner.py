import feedparser
import pandas as pd
import requests
from datetime import datetime
import time
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# News feeds organized by continent (excluding paid/API-required sources)
NEWS_FEEDS = {
    "North America": [
        {"name": "BBC News Americas", "url": "https://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml"},
        {"name": "CNN Top Stories", "url": "https://rss.cnn.com/rss/cnn_topstories.rss"},
        {"name": "NPR News", "url": "https://feeds.npr.org/1001/rss.xml"},
        {"name": "The New York Times", "url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml"},
        {"name": "Associated Press", "url": "https://rsshub.app/apnews/topics/apf-topnews"},
        {"name": "CBC News Canada", "url": "https://www.cbc.ca/webfeed/rss/rss-topstories"},
        {"name": "The Guardian US", "url": "https://www.theguardian.com/us-news/rss"},
        {"name": "Mexico News Daily", "url": "https://mexiconewsdaily.com/feed/"}
    ],
    "Europe": [
        {"name": "BBC News Europe", "url": "https://feeds.bbci.co.uk/news/world/europe/rss.xml"},
        {"name": "The Guardian UK", "url": "https://www.theguardian.com/uk/rss"},
        {"name": "Deutsche Welle English", "url": "https://rss.dw.com/rdf/rss-en-all"},
        {"name": "France24 English", "url": "https://www.france24.com/en/rss"},
        {"name": "El País English", "url": "https://feeds.elpais.com/mrss-s/pages/ep/site/english.elpais.com/portada"},
        {"name": "The Local Europe", "url": "https://www.thelocal.com/feed/"},
        {"name": "Euronews", "url": "https://www.euronews.com/rss"},
        {"name": "POLITICO Europe", "url": "https://www.politico.eu/feed/"},
        {"name": "RT Russia Today", "url": "https://www.rt.com/rss/news/"},
        {"name": "Swiss Info", "url": "https://www.swissinfo.ch/eng/latest-news/rss"}
    ],
    "Asia": [
        {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml"},
        {"name": "The Japan Times", "url": "https://www.japantimes.co.jp/feed/"},
        {"name": "South China Morning Post", "url": "https://www.scmp.com/rss/91/feed"},
        {"name": "The Hindu India", "url": "https://www.thehindu.com/news/national/feeder/default.rss"},
        {"name": "Channel NewsAsia", "url": "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml"},
        {"name": "Korea Herald", "url": "http://www.koreaherald.com/common/rss_xml.php"},
        {"name": "Times of India", "url": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms"},
        {"name": "Bangkok Post", "url": "https://www.bangkokpost.com/rss/data/topstories.xml"},
        {"name": "Arab News", "url": "https://www.arabnews.com/rss.xml"}
    ],
    "Africa": [
        {"name": "BBC News Africa", "url": "https://feeds.bbci.co.uk/news/world/africa/rss.xml"},
        {"name": "AllAfrica", "url": "https://allafrica.com/tools/headlines/rdf/latest/headlines.rdf"},
        {"name": "Mail & Guardian SA", "url": "https://mg.co.za/feed/"},
        {"name": "Daily Nation Kenya", "url": "https://nation.africa/kenya/rss"},
        {"name": "News24 South Africa", "url": "https://feeds.news24.com/articles/news24/TopStories/rss"},
        {"name": "Egypt Today", "url": "https://www.egypttoday.com/RSS/1/2/Egypt"},
        {"name": "Morocco World News", "url": "https://www.moroccoworldnews.com/feed/"},
        {"name": "Africa News", "url": "https://www.africanews.com/rss"}
    ],
    "South America": [
        {"name": "BBC News Latin America", "url": "https://feeds.bbci.co.uk/news/world/latin_america/rss.xml"},
        {"name": "Buenos Aires Times", "url": "https://www.batimes.com.ar/feed"},
        {"name": "The Rio Times", "url": "https://www.riotimesonline.com/feed/"},
        {"name": "MercoPress", "url": "https://en.mercopress.com/rss/v2/headlines"},
        {"name": "Colombia Reports", "url": "https://colombiareports.com/feed/"},
        {"name": "Peru Telegraph", "url": "https://www.perutelegraph.com/feed"},
        {"name": "Telesur English", "url": "https://www.telesurenglish.net/rss/RssPortada.html"}
    ],
    "Oceania": [
        {"name": "ABC News Australia", "url": "https://www.abc.net.au/news/feed/2942460/rss.xml"},
        {"name": "Sydney Morning Herald", "url": "https://www.smh.com.au/rss/feed.xml"},
        {"name": "NZ Herald", "url": "https://www.nzherald.co.nz/arc/outboundfeeds/rss/"},
        {"name": "The Guardian Australia", "url": "https://www.theguardian.com/australia-news/rss"},
        {"name": "The Age Melbourne", "url": "https://www.theage.com.au/rss/feed.xml"},
        {"name": "Stuff.co.nz", "url": "https://www.stuff.co.nz/rss"},
        {"name": "Radio New Zealand", "url": "https://www.rnz.co.nz/rss/national.xml"}
    ]
}

def extract_titles_from_feed(feed_url: str, feed_name: str, max_titles: int = 5) -> List[Dict]:
    """
    Extract titles from a single RSS/XML feed.
    
    Args:
        feed_url: URL of the RSS/XML feed
        feed_name: Name of the news source
        max_titles: Maximum number of titles to extract (default: 5)
    
    Returns:
        List of dictionaries containing title information
    """
    titles = []
    
    try:
        # Use feedparser to handle various RSS/XML formats
        feed = feedparser.parse(feed_url)
        
        # Check if feed was parsed successfully
        if feed.bozo:
            logging.warning(f"Feed parsing issue for {feed_name}: {feed.bozo_exception}")
        
        # Extract titles from entries
        entries = feed.entries[:max_titles] if hasattr(feed, 'entries') else []
        
        for entry in entries:
            title = None
            
            # Try different attributes where title might be stored
            if hasattr(entry, 'title'):
                title = entry.title
            elif hasattr(entry, 'title_detail'):
                title = entry.title_detail.value
            elif hasattr(entry, 'summary'):
                title = entry.summary[:100] + "..." if len(entry.summary) > 100 else entry.summary
            
            if title:
                titles.append({
                    'source': feed_name,
                    'title': title.strip(),
                    'timestamp': datetime.now().isoformat()
                })
        
        # If feedparser didn't find entries, try manual XML parsing
        if not titles:
            logging.info(f"Trying manual XML parsing for {feed_name}")
            titles = extract_titles_manual(feed_url, feed_name, max_titles)
            
    except Exception as e:
        logging.error(f"Error processing {feed_name}: {str(e)}")
    
    return titles

def extract_titles_manual(feed_url: str, feed_name: str, max_titles: int = 5) -> List[Dict]:
    """
    Manually extract titles using XML parsing as a fallback.
    """
    titles = []
    
    try:
        response = requests.get(feed_url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Common title tag names in different RSS/XML formats
        title_tags = ['title', '{http://purl.org/dc/elements/1.1/}title', 
                     '{http://www.w3.org/2005/Atom}title']
        
        count = 0
        for tag in title_tags:
            if count >= max_titles:
                break
                
            for elem in root.iter(tag):
                if elem.text and count < max_titles:
                    # Skip the feed title itself (usually the first title)
                    if count > 0 or elem.text != feed_name:
                        titles.append({
                            'source': feed_name,
                            'title': elem.text.strip(),
                            'timestamp': datetime.now().isoformat()
                        })
                        count += 1
                        
    except Exception as e:
        logging.error(f"Manual parsing failed for {feed_name}: {str(e)}")
    
    return titles

def scan_all_feeds(feeds_dict: Dict[str, List[Dict]], delay: float = 0.5) -> pd.DataFrame:
    """
    Scan all news feeds and compile results into a DataFrame.
    
    Args:
        feeds_dict: Dictionary of news feeds organized by continent
        delay: Delay between requests in seconds (default: 0.5)
    
    Returns:
        pandas DataFrame with all extracted titles
    """
    all_results = []
    total_feeds = sum(len(feeds) for feeds in feeds_dict.values())
    processed = 0
    
    for continent, feeds in feeds_dict.items():
        logging.info(f"\nProcessing {continent} feeds...")
        
        for feed in feeds:
            processed += 1
            logging.info(f"[{processed}/{total_feeds}] Fetching: {feed['name']}")
            
            titles = extract_titles_from_feed(feed['url'], feed['name'])
            
            # Add continent information to each title
            for title in titles:
                title['continent'] = continent
                title['feed_url'] = feed['url']
                all_results.append(title)
            
            # Be respectful of servers
            time.sleep(delay)
    
    # Create DataFrame
    df = pd.DataFrame(all_results)
    
    # Add some summary statistics
    if not df.empty:
        logging.info(f"\nScan complete!")
        logging.info(f"Total articles collected: {len(df)}")
        logging.info(f"Unique sources: {df['source'].nunique()}")
        logging.info(f"Articles per continent:")
        for continent in df['continent'].unique():
            count = len(df[df['continent'] == continent])
            logging.info(f"  {continent}: {count}")
    
    return df

def save_results(df: pd.DataFrame, filename: str = None):
    """
    Save the DataFrame to CSV and optionally to JSON.
    
    Args:
        df: DataFrame with results
        filename: Base filename (without extension)
    """
    if filename is None:
        filename = f"news_scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Save to CSV
    csv_file = f"{filename}.csv"
    df.to_csv(csv_file, index=False)
    logging.info(f"Results saved to {csv_file}")
    
    # Save to JSON for easier parsing in other applications
    json_file = f"{filename}.json"
    df.to_json(json_file, orient='records', indent=2)
    logging.info(f"Results also saved to {json_file}")

def main():
    """
    Main function to run the news feed scanner.
    """
    logging.info("Starting news feed scanner...")
    
    # Scan all feeds
    df = scan_all_feeds(NEWS_FEEDS)
    
    # Display sample results
    if not df.empty:
        print("\n=== Sample Results (First 10 titles) ===")
        print(df[['continent', 'source', 'title']].head(10).to_string(index=False))
        
        # Save results
        save_results(df)
        
        # Show feeds that failed to return titles
        all_sources = []
        for feeds in NEWS_FEEDS.values():
            all_sources.extend([f['name'] for f in feeds])
        
        successful_sources = df['source'].unique()
        failed_sources = set(all_sources) - set(successful_sources)
        
        if failed_sources:
            print(f"\n⚠️  Failed to retrieve titles from: {', '.join(failed_sources)}")
    else:
        logging.error("No titles were extracted from any feed!")
    
    return df

if __name__ == "__main__":
    # Run the scanner
    results_df = main()
    
    # You can access the DataFrame here for further processing
    # For example, filter for conversational pieces:
    # conversational_keywords = ['interview', 'conversation', 'talks', 'discusses', 'opinion']
    # pattern = '|'.join(conversational_keywords)
    # conversational_df = results_df[results_df['title'].str.contains(pattern, case=False, na=False)]