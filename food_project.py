import httpx
from models import Product
from database import SessionLocal

def fetch_products():
    url = "https://world.openfoodfacts.org/api/v0/product/3017620422003.json"  # Exemplo com um produto (Coca-Cola)
    
    response = httpx.get(url)
    data = response.json()
    
    if 'product' in data:
        product_data = data['product']
        db = SessionLocal()
        product = Product(
            name=product_data.get('product_name', 'N/A'),
            brand=product_data.get('brands', 'N/A'),
            category=product_data.get('categories', 'N/A'),
            code=product_data.get('code')
        )
        db.add(product)
        db.commit()
        db.close()
