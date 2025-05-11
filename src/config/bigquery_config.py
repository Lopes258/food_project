import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Carregar as variáveis de ambiente do arquivo bigquery.env
load_dotenv(
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "bigquery.env"
    )
)

# Configuração do BigQuery
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
if not PROJECT_ID:
    raise ValueError(
        "GOOGLE_CLOUD_PROJECT environment variable is not set in bigquery.env"
    )

CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not CREDENTIALS_PATH:
    raise ValueError(
        "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set in bigquery.env"
    )

if not os.path.exists(CREDENTIALS_PATH):
    raise FileNotFoundError(
        f"Service account key file not found at: {CREDENTIALS_PATH}"
    )

DATASET_ID = "food_data"
TABLE_ID = "products"


# Inicializar o cliente do BigQuery
def get_bigquery_client():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    except Exception as e:
        raise Exception(f"Erro ao inicializar o cliente do BigQuery: {str(e)}")


# Schema para a tabela de produtos
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
    bigquery.SchemaField("sugars_100g", "FLOAT"),
]
