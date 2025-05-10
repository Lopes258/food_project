import requests
import json
import pandas as pd
from tqdm import tqdm
import time
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.cloud import translate
from bigquery_config import get_bigquery_client, PROJECT_ID, DATASET_ID, TABLE_ID, PRODUCTS_SCHEMA
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from astral import LocationInfo
from astral.sun import sun

class OpenFoodFactsAPI:
    BASE_URL = "https://world.openfoodfacts.org/api/v2"
    TIMEOUT = aiohttp.ClientTimeout(total=30)  # 30 seconds timeout
    
    def __init__(self):
        self.session = requests.Session()
        self.async_session = None
    
    async def init_async_session(self):
        if not self.async_session:
            self.async_session = aiohttp.ClientSession(timeout=self.TIMEOUT)
    
    async def close_async_session(self):
        if self.async_session:
            await self.async_session.close()
            self.async_session = None
    
    async def get_product_async(self, barcode: str) -> Dict[str, Any]:
        """Fetch a single product by barcode asynchronously"""
        try:
            await self.init_async_session()
            url = f"{self.BASE_URL}/product/{barcode}.json"
            async with self.async_session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('product', {})
                return {}
        except Exception as e:
            print(f"Error fetching product {barcode}: {str(e)}")
            return {}
    
    async def search_products_async(self, query: str, page: int = 1, page_size: int = 100) -> List[Dict[str, Any]]:
        """Search for products with pagination asynchronously"""
        try:
            await self.init_async_session()
            url = f"{self.BASE_URL}/search"
            params = {
                'search_terms': query,
                'page': page,
                'page_size': page_size
            }
            async with self.async_session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('products', [])
                return []
        except Exception as e:
            print(f"Error searching products for query '{query}' on page {page}: {str(e)}")
            return []

def translate_text(text: str, target_language: str = 'en') -> str:
    """Translate text to target language using Google Cloud Translation API"""
    if not text:
        return ""
    
    try:
        # Initialize the translation client
        translate_client = translate.TranslationServiceClient()
        
        # Set the project and location
        project_id = PROJECT_ID
        location = "global"
        parent = f"projects/{project_id}/locations/{location}"
        
        # Translate the text
        response = translate_client.translate_text(
            request={
                "parent": parent,
                "contents": [text],
                "mime_type": "text/plain",
                "target_language_code": target_language,
            }
        )
        
        # Get the translated text
        return response.translations[0].translated_text
    except Exception as e:
        print(f"Error translating text: {str(e)}")
        return text  # Return original text if translation fails

def process_product_data(product_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process raw product data into database format"""
    # Generate a unique ID based on the product code
    product_id = int(product_data.get('code', '0')) if product_data.get('code', '').isdigit() else 0
    
    # Get current time in UTC
    now = datetime.now(timezone.utc)
    
    return {
        'id': product_id,
        'code': product_data.get('code'),
        'product_name': product_data.get('product_name'),
        'brands': product_data.get('brands'),
        'categories': product_data.get('categories'),
        'ingredients_text': product_data.get('ingredients_text', ''),  # Use original ingredients text
        'nutriments': json.dumps(product_data.get('nutriments', {})),
        'quantity': product_data.get('quantity'),
        'serving_size': product_data.get('serving_size'),
        'energy_100g': product_data.get('nutriments', {}).get('energy-kcal_100g'),
        'proteins_100g': product_data.get('nutriments', {}).get('proteins_100g'),
        'carbohydrates_100g': product_data.get('nutriments', {}).get('carbohydrates_100g'),
        'fat_100g': product_data.get('nutriments', {}).get('fat_100g'),
        'fiber_100g': product_data.get('nutriments', {}).get('fiber_100g'),
        'sodium_100g': product_data.get('nutriments', {}).get('sodium_100g'),
        'sugars_100g': product_data.get('nutriments', {}).get('sugars_100g'),
        'created_datetime': now,
        'last_modified_datetime': now
    }

def setup_bigquery():
    """Set up BigQuery dataset and table if they don't exist"""
    client = get_bigquery_client()
    
    # Create dataset if it doesn't exist
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_ref} already exists")
    except Exception:
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset = client.create_dataset(dataset)
            print(f"Created dataset {dataset_ref}")
        except Exception as e:
            print(f"Error creating dataset: {str(e)}")
            raise
    
    # Create or recreate table
    table_ref = f"{dataset_ref}.{TABLE_ID}"
    try:
        # Delete table if it exists
        client.delete_table(table_ref)
        print(f"Deleted existing table {table_ref}")
    except Exception:
        print(f"Table {table_ref} did not exist")
    
    try:
        # Create new table with updated schema
        table = bigquery.Table(table_ref, schema=PRODUCTS_SCHEMA)
        table = client.create_table(table)
        print(f"Created table {table_ref}")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        raise

def get_sun_times():
    """Get sunrise and sunset times for the current location"""
    # Default to Sao Paulo coordinates
    city = LocationInfo("Sao Paulo", "Brazil", "America/Sao_Paulo", -23.5505, -46.6333)
    s = sun(city.observer, date=datetime.now(timezone.utc))
    return {
        'sunrise': s['sunrise'].replace(tzinfo=timezone.utc),
        'sunset': s['sunset'].replace(tzinfo=timezone.utc),
        'noon': s['noon'].replace(tzinfo=timezone.utc),
        'dawn': s['dawn'].replace(tzinfo=timezone.utc),
        'dusk': s['dusk'].replace(tzinfo=timezone.utc)
    }

def is_daytime() -> bool:
    """Check if it's currently daytime"""
    try:
        sun_times = get_sun_times()
        now = datetime.now(timezone.utc)
        return sun_times['dawn'] <= now <= sun_times['dusk']
    except Exception as e:
        print(f"Error checking daytime: {str(e)}")
        return True  # Default to True if there's an error

def get_next_sunrise() -> datetime:
    """Get the time of the next sunrise"""
    try:
        sun_times = get_sun_times()
        now = datetime.now(timezone.utc)
        if now > sun_times['sunrise']:
            # If we're past today's sunrise, get tomorrow's
            tomorrow = now + timedelta(days=1)
            city = LocationInfo("Sao Paulo", "Brazil", "America/Sao_Paulo", -23.5505, -46.6333)
            s = sun(city.observer, date=tomorrow)
            return s['sunrise'].replace(tzinfo=timezone.utc)
        return sun_times['sunrise']
    except Exception as e:
        print(f"Error getting next sunrise: {str(e)}")
        return datetime.now(timezone.utc) + timedelta(hours=24)  # Default to 24 hours from now

async def fetch_and_store_data_async(api: OpenFoodFactsAPI, search_term: str = "coca", max_pages: int = 10):
    """Fetch and store products in BigQuery asynchronously"""
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    try:
        # Create tasks for all pages
        tasks = []
        for page in range(1, max_pages + 1):
            tasks.append(api.search_products_async(search_term, page=page))
        
        # Execute all tasks concurrently with timeout
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process all results
        all_processed_products = []
        for page_num, result in enumerate(results, 1):
            if isinstance(result, Exception):
                print(f"Error on page {page_num}: {str(result)}")
                continue
                
            products = result
            if not products:
                print(f"No more products found after page {page_num}")
                break
                
            # Process products
            for product_data in products:
                processed_data = process_product_data(product_data)
                if processed_data['code']:  # Only store if we have a product code
                    all_processed_products.append(processed_data)
            
            print(f"Found {len(products)} products on page {page_num}")
        
        if all_processed_products:
            # Convert to DataFrame
            df = pd.DataFrame(all_processed_products)
            
            # Upload to BigQuery
            job_config = bigquery.LoadJobConfig(
                schema=PRODUCTS_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            try:
                job = client.load_table_from_dataframe(
                    df, table_ref, job_config=job_config
                )
                job.result()  # Wait for the job to complete
                print(f"Successfully uploaded {len(all_processed_products)} products to BigQuery")
            except Exception as e:
                print(f"Error uploading to BigQuery: {str(e)}")
    except Exception as e:
        print(f"Error in fetch_and_store_data_async: {str(e)}")
    finally:
        # Ensure we close the session
        await api.close_async_session()

async def main_async():
    print("Setting up BigQuery...")
    # Set up BigQuery
    setup_bigquery()
    
    print("\nInitializing API...")
    # Initialize API
    api = OpenFoodFactsAPI()
    
    try:
        print("\nStarting data collection...")
        # Get current time information
        sun_times = get_sun_times()
        now = datetime.now(timezone.utc)
        print(f"Current time (UTC): {now}")
        print(f"Sunrise (UTC): {sun_times['sunrise']}")
        print(f"Sunset (UTC): {sun_times['sunset']}")
        
        # List of common food categories to search
        search_terms = [
            "food", "snack", "drink", "beverage", "cereal", "bread", "meat", 
            "vegetable", "fruit", "dairy", "sweet", "candy", "chocolate",
            "pasta", "rice", "sauce", "soup", "breakfast", "lunch", "dinner"
        ]
        
        # Process search terms in smaller batches to avoid overwhelming the API
        batch_size = 3  # Reduced batch size to avoid connection issues
        for i in range(0, len(search_terms), batch_size):
            batch = search_terms[i:i + batch_size]
            print(f"\nProcessing batch {i//batch_size + 1} of {(len(search_terms) + batch_size - 1)//batch_size}")
            
            # Create tasks for current batch
            tasks = []
            for term in batch:
                print(f"Searching for products with term: {term}")
                tasks.append(fetch_and_store_data_async(api, search_term=term, max_pages=50))
            
            # Execute batch tasks concurrently
            await asyncio.gather(*tasks)
            
            # Add a longer delay between batches
            await asyncio.sleep(5)  # Increased delay between batches
            
            # Check if it's getting close to sunset
            if not is_daytime():
                next_sunrise = get_next_sunrise()
                print(f"\nIt's nighttime. Next data collection will be at sunrise: {next_sunrise}")
                break
    
    except Exception as e:
        print(f"Error in main_async: {str(e)}")
    finally:
        # Ensure we close the session
        await api.close_async_session()
    
    print("\nData collection complete!")

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()
