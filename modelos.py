from sqlalchemy import Column, Integer, String
from database import Base

class Product(Base):
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    brand = Column(String)
    category = Column(String)
    code = Column(String, unique=True, index=True)
