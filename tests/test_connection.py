from bigquery_config import get_bigquery_client, PROJECT_ID, DATASET_ID

def test_connection():
    try:
        # Get BigQuery client
        client = get_bigquery_client()
        
        # List datasets to verify connection
        datasets = list(client.list_datasets())
        
        print(f"Successfully connected to BigQuery!")
        print(f"Project ID: {PROJECT_ID}")
        print(f"Available datasets: {[dataset.dataset_id for dataset in datasets]}")
        
        return True
    except Exception as e:
        print(f"Error connecting to BigQuery: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection() 