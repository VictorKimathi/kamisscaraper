"""
KAMIS Scraper - Database Version
Scrapes agricultural commodity prices and stores them in Supabase PostgreSQL
"""

import os
import re
import sys
import time
import logging
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://kamis.kilimo.go.ke/site/market"
BATCH_SIZE = 100  # Insert records in batches
REQUEST_DELAY = 1.5  # Seconds between requests
MAX_RETRIES = 3

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("Missing Supabase credentials in .env file")


@dataclass
class ScrapedRecord:
    """Represents a single price record from KAMIS"""
    product_name: str
    market_name: str
    county_name: str
    classification: Optional[str]
    grade: Optional[str]
    sex: Optional[str]
    wholesale_price: Optional[Decimal]
    retail_price: Optional[Decimal]
    supply_volume: Optional[Decimal]
    price_date: date


class DatabaseManager:
    """Handles all database operations with Supabase"""
    
    def __init__(self):
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        self._cache = {
            'counties': {},
            'commodities': {},
            'markets': {},
            'categories': {}
        }
        self._initialize_categories()
    
    def _initialize_categories(self):
        """Pre-populate commodity categories if empty"""
        categories = ['Grains', 'Vegetables', 'Fruits', 'Livestock', 'Fish', 'Other']
        for cat_name in categories:
            existing = self.supabase.table('commodity_categories')\
                .select('id').eq('name', cat_name).execute()
            if not existing.data:
                self.supabase.table('commodity_categories')\
                    .insert({'name': cat_name}).execute()
                logger.info(f"Created category: {cat_name}")
    
    def _get_or_create_county(self, name: str) -> int:
        """Get county ID or create if not exists"""
        name = name.strip().title()
        if name in self._cache['counties']:
            return self._cache['counties'][name]
        
        # Try to find existing
        result = self.supabase.table('counties').select('id').eq('name', name).execute()
        if result.data:
            county_id = result.data[0]['id']
        else:
            # Create new
            result = self.supabase.table('counties').insert({'name': name}).execute()
            county_id = result.data[0]['id']
            logger.info(f"Created new county: {name}")
        
        self._cache['counties'][name] = county_id
        return county_id
    
    def _get_or_create_commodity(self, product_name: str) -> int:
        """Get commodity ID or create if not exists"""
        name = product_name.strip().title()
        if name in self._cache['commodities']:
            return self._cache['commodities'][name]
        
        # Determine category based on product name keywords
        category_id = self._categorize_product(name)
        
        # Try to find existing
        result = self.supabase.table('commodities')\
            .select('id').eq('name', name).execute()
        
        if result.data:
            commodity_id = result.data[0]['id']
        else:
            # Create new
            result = self.supabase.table('commodities').insert({
                'name': name,
                'category_id': category_id
            }).execute()
            commodity_id = result.data[0]['id']
            logger.info(f"Created new commodity: {name} (Category ID: {category_id})")
        
        self._cache['commodities'][name] = commodity_id
        return commodity_id
    
    def _categorize_product(self, product_name: str) -> Optional[int]:
        """Categorize product based on name keywords"""
        name_lower = product_name.lower()
        
        category_map = {
            'Grains': ['maize', 'beans', 'rice', 'wheat', 'sorghum', 'millet', 'greengrams', 'ndengu'],
            'Vegetables': ['cabbage', 'kale', 'spinach', 'tomato', 'onion', 'potato', 'carrot', 'pepper', 'eggplant', 'lettuce'],
            'Fruits': ['mango', 'banana', 'orange', 'apple', 'pineapple', 'watermelon', 'avocado', 'passion'],
            'Livestock': ['cattle', 'goat', 'sheep', 'pig', 'chicken', 'broiler', 'layer', 'beef', 'mutton'],
            'Fish': ['fish', 'tilapia', 'nile perch', 'omena', 'sardine']
        }
        
        for category, keywords in category_map.items():
            if any(keyword in name_lower for keyword in keywords):
                result = self.supabase.table('commodity_categories')\
                    .select('id').eq('name', category).execute()
                if result.data:
                    return result.data[0]['id']
        
        # Default to 'Other'
        result = self.supabase.table('commodity_categories')\
            .select('id').eq('name', 'Other').execute()
        return result.data[0]['id'] if result.data else None
    
    def _get_or_create_market(self, market_name: str, county_name: str) -> int:
        """Get market ID or create if not exists"""
        cache_key = f"{market_name.strip().title()}_{county_name.strip().title()}"
        if cache_key in self._cache['markets']:
            return self._cache['markets'][cache_key]
        
        county_id = self._get_or_create_county(county_name)
        market_name_clean = market_name.strip().title()
        
        # Try to find existing
        result = self.supabase.table('markets')\
            .select('id')\
            .eq('name', market_name_clean)\
            .eq('county_id', county_id)\
            .execute()
        
        if result.data:
            market_id = result.data[0]['id']
        else:
            # Create new
            result = self.supabase.table('markets').insert({
                'name': market_name_clean,
                'county_id': county_id
            }).execute()
            market_id = result.data[0]['id']
            logger.info(f"Created new market: {market_name_clean} in {county_name}")
        
        self._cache['markets'][cache_key] = market_id
        return market_id
    
    def insert_price_records(self, records: List[ScrapedRecord]) -> int:
        """Insert price records in batches, skipping duplicates"""
        if not records:
            return 0
        
        inserted_count = 0
        batch = []
        
        for record in records:
            try:
                commodity_id = self._get_or_create_commodity(record.product_name)
                market_id = self._get_or_create_market(record.market_name, record.county_name)
                
                # Check for existing record to avoid duplicates
                existing = self.supabase.table('price_records')\
                    .select('id')\
                    .eq('commodity_id', commodity_id)\
                    .eq('market_id', market_id)\
                    .eq('record_date', record.price_date.isoformat())\
                    .execute()
                
                if existing.data:
                    logger.debug(f"Skipping duplicate: {record.product_name} at {record.market_name} on {record.price_date}")
                    continue
                
                batch.append({
                    'commodity_id': commodity_id,
                    'market_id': market_id,
                    'classification': record.classification,
                    'grade': record.grade,
                    'sex': record.sex,
                    'wholesale_price': float(record.wholesale_price) if record.wholesale_price else None,
                    'retail_price': float(record.retail_price) if record.retail_price else None,
                    'supply_volume': float(record.supply_volume) if record.supply_volume else None,
                    'record_date': record.price_date.isoformat()
                })
                
                # Insert in batches
                if len(batch) >= BATCH_SIZE:
                    inserted_count += self._execute_batch_insert(batch)
                    batch = []
                    
            except Exception as e:
                logger.error(f"Error preparing record for insert: {e}")
                continue
        
        # Insert remaining
        if batch:
            inserted_count += self._execute_batch_insert(batch)
        
        return inserted_count
    
    def _execute_batch_insert(self, batch: List[Dict]) -> int:
        """Execute batch insert with error handling"""
        try:
            result = self.supabase.table('price_records').insert(batch).execute()
            logger.info(f"Inserted batch of {len(result.data)} records")
            return len(result.data)
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            # Try individual inserts as fallback
            success_count = 0
            for item in batch:
                try:
                    self.supabase.table('price_records').insert(item).execute()
                    success_count += 1
                except Exception as inner_e:
                    logger.error(f"Individual insert failed: {inner_e}")
            return success_count


class KAMISScraper:
    """Main scraper class for KAMIS website"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.db = DatabaseManager()
    
    @retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _fetch(self, url: str) -> BeautifulSoup:
        """Fetch URL with retry logic"""
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    
    def get_product_list(self) -> List[Dict]:
        """Fetch available products from dropdown"""
        logger.info("Fetching product list...")
        try:
            soup = self._fetch(BASE_URL)
            select = soup.find('select', {'name': 'product'})
            
            if not select:
                raise ValueError("Could not find product dropdown")
            
            products = []
            for opt in select.find_all('option'):
                value = opt.get('value')
                if value and value.isdigit():
                    products.append({
                        'id': int(value),
                        'name': opt.text.strip()
                    })
            
            logger.info(f"Found {len(products)} products")
            return products
            
        except Exception as e:
            logger.error(f"Failed to fetch product list: {e}")
            raise
    
    def _parse_price(self, value: str) -> Optional[Decimal]:
        """Parse price string to Decimal"""
        if not value or value in ('-', '', 'N/A'):
            return None
        # Remove non-numeric characters except decimal point
        cleaned = re.sub(r'[^\d.]', '', value)
        try:
            return Decimal(cleaned) if cleaned else None
        except InvalidOperation:
            return None
    
    def _parse_volume(self, value: str) -> Optional[Decimal]:
        """Parse volume/supply string to Decimal"""
        if not value or value in ('-', '', 'N/A'):
            return None
        # Extract number from strings like "5000 kg", "High", "Low"
        numbers = re.findall(r'\d+', value.replace(',', ''))
        if numbers:
            try:
                return Decimal(numbers[0])
            except InvalidOperation:
                return None
        return None
    
    def _parse_date(self, value: str) -> date:
        """Parse date string to date object"""
        # Try common formats
        formats = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%m/%d/%Y']
        for fmt in formats:
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
        # Default to today if parsing fails
        logger.warning(f"Could not parse date: {value}, using today")
        return date.today()
    
    def _extract_county_from_market(self, market_text: str) -> Tuple[str, str]:
        """Extract market name and county from combined text"""
        # Common patterns: "Market Name - County" or "Market Name (County)"
        patterns = [
            r'(.+?)\s*[-–]\s*(.+)',
            r'(.+?)\s*\((.+?)\)',
            r'(.+?)\s*,\s*(.+)'
        ]
        
        for pattern in patterns:
            match = re.match(pattern, market_text)
            if match:
                return match.group(1).strip(), match.group(2).strip()
        
        # If no pattern matches, return as-is with unknown county
        return market_text.strip(), "Unknown"
    
    def scrape_product(self, product_id: int, product_name: str, per_page: int = 100) -> List[ScrapedRecord]:
        """Scrape all data for a specific product"""
        url = f"{BASE_URL}?product={product_id}&per_page={per_page}"
        logger.info(f"Scraping: {product_name} (ID: {product_id})")
        
        try:
            soup = self._fetch(url)
            table = soup.find('table', class_='table table-bordered table-condensed')
            
            if not table:
                logger.warning(f"No data table found for {product_name}")
                return []
            
            # Extract headers
            headers = []
            thead = table.find('thead')
            if thead:
                headers = [th.get_text(strip=True).lower() for th in thead.find_all('th')]
            
            # Map common header variations
            header_map = {
                'market': ['market', 'market name', 'soko'],
                'county': ['county', 'region', 'county name'],
                'classification': ['classification', 'variety', 'type'],
                'grade': ['grade', 'quality'],
                'sex': ['sex', 'gender'],
                'wholesale': ['wholesale price', 'wholesale', 'w/sale price'],
                'retail': ['retail price', 'retail', 'price'],
                'volume': ['supply volume', 'volume', 'quantity', 'supply'],
                'date': ['date', 'price date', 'recorded date']
            }
            
            def find_column_index(target):
                for key, variants in header_map.items():
                    if target == key:
                        for i, h in enumerate(headers):
                            if any(v in h for v in variants):
                                return i
                return -1
            
            idx_map = {
                'market': find_column_index('market'),
                'county': find_column_index('county'),
                'classification': find_column_index('classification'),
                'grade': find_column_index('grade'),
                'sex': find_column_index('sex'),
                'wholesale': find_column_index('wholesale'),
                'retail': find_column_index('retail'),
                'volume': find_column_index('volume'),
                'date': find_column_index('date')
            }
            
            records = []
            tbody = table.find('tbody')
            if not tbody:
                return []
            
            for row in tbody.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) < 3:  # Skip malformed rows
                    continue
                
                # Extract market and county
                market_text = cells[idx_map['market']].get_text(strip=True) if idx_map['market'] >= 0 else ""
                
                # If county column exists use it, otherwise extract from market text
                if idx_map['county'] >= 0:
                    market_name = market_text
                    county_name = cells[idx_map['county']].get_text(strip=True)
                else:
                    market_name, county_name = self._extract_county_from_market(market_text)
                
                # Parse date
                date_text = cells[idx_map['date']].get_text(strip=True) if idx_map['date'] >= 0 else str(date.today())
                price_date = self._parse_date(date_text)
                
                record = ScrapedRecord(
                    product_name=product_name,
                    market_name=market_name,
                    county_name=county_name or "Unknown",
                    classification=cells[idx_map['classification']].get_text(strip=True) if idx_map['classification'] >= 0 else None,
                    grade=cells[idx_map['grade']].get_text(strip=True) if idx_map['grade'] >= 0 else None,
                    sex=cells[idx_map['sex']].get_text(strip=True) if idx_map['sex'] >= 0 else None,
                    wholesale_price=self._parse_price(
                        cells[idx_map['wholesale']].get_text(strip=True) if idx_map['wholesale'] >= 0 else ""
                    ),
                    retail_price=self._parse_price(
                        cells[idx_map['retail']].get_text(strip=True) if idx_map['retail'] >= 0 else ""
                    ),
                    supply_volume=self._parse_volume(
                        cells[idx_map['volume']].get_text(strip=True) if idx_map['volume'] >= 0 else ""
                    ),
                    price_date=price_date
                )
                
                records.append(record)
            
            logger.info(f"Scraped {len(records)} records for {product_name}")
            return records
            
        except Exception as e:
            logger.error(f"Error scraping {product_name}: {e}")
            return []
    
    def run(self, specific_products: Optional[List[str]] = None):
        """Main execution method"""
        try:
            products = self.get_product_list()
            
            if specific_products:
                products = [p for p in products if p['name'] in specific_products]
            
            total_inserted = 0
            
            for i, product in enumerate(products, 1):
                logger.info(f"Processing {i}/{len(products)}: {product['name']}")
                
                # Scrape
                records = self.scrape_product(product['id'], product['name'])
                
                if records:
                    # Insert to database
                    inserted = self.db.insert_price_records(records)
                    total_inserted += inserted
                    logger.info(f"Inserted {inserted} new records for {product['name']}")
                
                # Polite delay
                if i < len(products):
                    time.sleep(REQUEST_DELAY)
            
            logger.info(f"Scraping complete. Total new records inserted: {total_inserted}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Scraper failed: {e}")
            raise


def main():
    """Entry point"""
    scraper = KAMISScraper()
    
    # Optional: Scrape only specific products for testing
    # test_products = ["Dry Maize", "Beans"]
    # scraper.run(specific_products=test_products)
    
    # Scrape all products
    scraper.run()


if __name__ == "__main__":
    main()