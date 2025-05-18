# Сервер на FastAPI для ЛР4
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
# --- Моделі даних (Pydantic) ---
# Модель для створення продукту (без id, бо він генерується)
class ProductCreate(BaseModel):
    name: str = Field(..., min_length=3, example="Smartphone Alpha")
    category: str = Field(..., example="Smartphones")
    price: float = Field(..., gt=0, example=15999.99)
    stock: int = Field(..., ge=0, example=50)
# Модель для оновлення продукту (всі поля опціональні)
class ProductUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=3, example="Smartphone Alpha X")
    category: Optional[str] = Field(None, example="Smartphones")
    price: Optional[float] = Field(None, gt=0, example=16500.00)
    stock: Optional[int] = Field(None, ge=0, example=45)
# Модель продукту для відповіді (включає id)
class Product(ProductCreate):
    id: int
# --- "In-memory" база даних ---
# Використовуємо словник для зберігання продуктів, де ключ - це id
products_db: Dict[int, Product] = {}
# Лічильник для генерації наступного унікального ID
next_product_id: int = 1
# --- Ініціалізація FastAPI ---
app = FastAPI(
    title="Electronics Shop API",
    description="REST API to manage the products catalog.",
    version="1.0.0"
)
# --- API endpoints ---
@app.post("/products", response_model=Product, status_code=status.HTTP_201_CREATED, tags=["Products"])
async def create_product(product_in: ProductCreate):
    """
    Створює новий товар в каталозі.
    Приймає дані товару, генерує ID, зберігає та повертає створений товар.
    """
    global next_product_id
    product_id = next_product_id
    # Створюємо об'єкт Product з присвоєним id
    new_product = Product(id=product_id, **product_in.dict())
    products_db[product_id] = new_product
    next_product_id += 1
    return new_product

@app.get("/products", response_model=List[Product], tags=["Products"])
async def get_products(category: Optional[str] = Query(None, description="Filter products by category")):
    """
    Повертає список всіх товарів.
    Можна опціонально фільтрувати за категорією.
    """
    if category:
        # Фільтруємо продукти за категорією
        filtered_products = [
            product for product in products_db.values() if product.category.lower() == category.lower()
        ]
        return filtered_products
    # Повертаємо всі продукти, якщо категорія не вказана
    return list(products_db.values())

@app.get("/products/{product_id}", response_model=Product, tags=["Products"])
async def get_product(product_id: int):
    """
    Повертає інформацію про конкретний товар за його ID.
    Якщо товар не знайдено, повертає помилку 404.
    """
    product = products_db.get(product_id)
    if product is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    return product

@app.put("/products/{product_id}", response_model=Product, tags=["Products"])
async def update_product(product_id: int, product_in: ProductUpdate):
    """
    Оновлює інформацію про існуючий товар за його ID.
    Приймає дані для оновлення (лише ті поля, що потрібно змінити).
    Якщо товар не знайдено, повертає помилку 404.
    """
    existing_product = products_db.get(product_id)
    if existing_product is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    # Оновлюємо дані існуючого продукту
    update_data = product_in.dict(exclude_unset=True) # Беремо лише передані поля
    for key, value in update_data.items():
        setattr(existing_product, key, value)
    products_db[product_id] = existing_product # Зберігаємо оновлений об'єкт
    return existing_product

@app.delete("/products/{product_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Products"])
async def delete_product(product_id: int):
    """
    Видаляє товар з каталогу за його ID.
    Якщо товар не знайдено, повертає помилку 404.
    У разі успіху повертає статус 204 No Content.
    """
    if product_id not in products_db:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    del products_db[product_id]
    # Успішна відповідь DELETE не повинна мати тіла
    return None

# --- Запуск сервера (для локальної розробки) ---
# To run this server, use the command in the terminal:
# uvicorn lab_4:app --reload
# Where 'lab_4' is your filename (lab_4.py), 'app' is the FastAPI object.
# The '--reload' flag will automatically reload the server on code changes.
# API will be available at http://127.0.0.1:8000
# Interactive documentation (Swagger UI): http://127.0.0.1:8000/docs
# Alternative documentation (ReDoc): http://127.0.0.1:8000/redoc