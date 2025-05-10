from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

# Load environment variables from bigquery.env
load_dotenv('bigquery.env')

# BigQuery configuration
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
if not PROJECT_ID:
    raise ValueError("GOOGLE_CLOUD_PROJECT environment variable is not set in bigquery.env")

CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if not CREDENTIALS_PATH:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set in bigquery.env")

if not os.path.exists(CREDENTIALS_PATH):
    raise FileNotFoundError(f"Service account key file not found at: {CREDENTIALS_PATH}")

DATASET_ID = 'food_data'
TABLE_ID = 'products'

# Initialize BigQuery client
def get_bigquery_client():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    except Exception as e:
        raise Exception(f"Failed to initialize BigQuery client: {str(e)}")

# Schema for the products table
PRODUCTS_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("brands", "STRING"),
    bigquery.SchemaField("categories", "STRING"),
    bigquery.SchemaField("ingredients_text", "STRING"),  # Original ingredients text
    bigquery.SchemaField("nutriments", "STRING"),
    bigquery.SchemaField("created_datetime", "TIMESTAMP"),
    bigquery.SchemaField("last_modified_datetime", "TIMESTAMP"),
    bigquery.SchemaField("quantity", "STRING"),
    bigquery.SchemaField("serving_size", "STRING"),
    bigquery.SchemaField("energy_100g", "FLOAT"),
    bigquery.SchemaField("proteins_100g", "FLOAT"),
    bigquery.SchemaField("carbohydrates_100g", "FLOAT"),
    bigquery.SchemaField("fat_100g", "FLOAT"),
    bigquery.SchemaField("fiber_100g", "FLOAT"),
    bigquery.SchemaField("sodium_100g", "FLOAT"),
    bigquery.SchemaField("sugars_100g", "FLOAT")
] 