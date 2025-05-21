# federation_service.py

import json
import os
import logging
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

# --- Налаштування логування ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FederationService")

# --- Шляхи до файлів-джерел ---
PRODUCTS_SOURCE_PATH = "products_source.json"
INVENTORY_SOURCE_PATH = "inventory_source.json"

# --- Моделі даних (Pydantic) ---
# Модель для глобального (федеративного) представлення продукту
class FederatedProduct(BaseModel):
    id: int
    name: str
    category: str
    price: float
    stock: Optional[int] = None # Може бути відсутнім, якщо немає в інвентарі
    supplier: Optional[str] = None
    location: Optional[str] = None

# --- Функція для створення/завантаження тестових даних ---
def setup_mock_data():
    # Дані для products_source.json
    products_data = [
        {"product_id": 101, "name": "Laptop Pro X17", "category": "Laptops", "base_price": 35000.00},
        {"product_id": 102, "name": "Smartphone G-Ultra", "category": "Smartphones", "base_price": 22000.00},
        {"product_id": 103, "name": "Wireless Headset AirSound", "category": "Audio", "base_price": 4500.00},
        {"product_id": 104, "name": "4K OLED Monitor ViewMax", "category": "Monitors", "base_price": 18000.00},
        {"product_id": 105, "name": "Gaming Keyboard MechPro", "category": "Accessories", "base_price": 3200.00}
    ]
    # Дані для inventory_source.json
    inventory_data = [
        {"item_id": 101, "stock_quantity": 15, "supplier": "TechGlobal", "warehouse_location": "Warehouse A"},
        {"item_id": 102, "stock_quantity": 30, "supplier": "MobileCorp", "warehouse_location": "Warehouse B"},
        {"item_id": 103, "stock_quantity": 50, "supplier": "AudioWorld", "warehouse_location": "Warehouse A"},
        # Зверніть увагу: для продукту 104 немає запису в інвентарі
        {"item_id": 105, "stock_quantity": 25, "supplier": "GamingGear Inc.", "warehouse_location": "Warehouse C"}
    ]

    if not os.path.exists(PRODUCTS_SOURCE_PATH):
        with open(PRODUCTS_SOURCE_PATH, 'w', encoding='utf-8') as f:
            json.dump(products_data, f, indent=4, ensure_ascii=False)
        logger.info(f"Created mock data file: {PRODUCTS_SOURCE_PATH}")

    if not os.path.exists(INVENTORY_SOURCE_PATH):
        with open(INVENTORY_SOURCE_PATH, 'w', encoding='utf-8') as f:
            json.dump(inventory_data, f, indent=4, ensure_ascii=False)
        logger.info(f"Created mock data file: {INVENTORY_SOURCE_PATH}")

# --- Ініціалізація FastAPI ---
app = FastAPI(
    title="Data Federation Service - Electronics Store",
    description="Demonstrates data federation by combining product and inventory data.",
    version="1.0.0"
)

# --- Логіка доступу до джерел даних ---
def get_product_details(product_id: int) -> Optional[Dict[str, Any]]:
    """Отримує деталі продукту з products_source.json."""
    try:
        with open(PRODUCTS_SOURCE_PATH, 'r', encoding='utf-8') as f:
            products = json.load(f)
        for p in products:
            if p.get("product_id") == product_id:
                return p
    except FileNotFoundError:
        logger.error(f"File not found: {PRODUCTS_SOURCE_PATH}")
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {PRODUCTS_SOURCE_PATH}")
    return None

def get_inventory_details(item_id: int) -> Optional[Dict[str, Any]]:
    """Отримує деталі інвентарю з inventory_source.json."""
    try:
        with open(INVENTORY_SOURCE_PATH, 'r', encoding='utf-8') as f:
            inventory = json.load(f)
        for i in inventory:
            if i.get("item_id") == item_id:
                return i
    except FileNotFoundError:
        logger.error(f"File not found: {INVENTORY_SOURCE_PATH}")
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {INVENTORY_SOURCE_PATH}")
    return None

# --- Ендпоінт API для федеративного запиту ---
@app.get("/federated/products/{product_id}",
            response_model=FederatedProduct,
            summary="Get federated product information",
            tags=["Federated Products"])
async def get_federated_product(product_id: int):
    """
    Отримує консолідовану (федеративну) інформацію про продукт,
    динамічно поєднуючи дані з основного каталогу продуктів та системи інвентарю.
    Це симулює підхід Global as View (GAV), де глобальна схема
    (FederatedProduct) визначається на основі запитів до локальних джерел.
    """
    logger.info(f"Federated query received for product_id: {product_id}")

    # 1. Видобування даних з першого джерела (основний каталог продуктів)
    product_info = get_product_details(product_id)

    if not product_info:
        logger.warning(f"Product with id {product_id} not found in primary source.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found in primary catalog")

    # 2. Видобування даних з другого джерела (інвентар)
    inventory_info = get_inventory_details(product_id) # item_id відповідає product_id

    # 3. Трансформація/Комбінування даних для формування глобального представлення
    # Це і є "відображення даних під час запиту" [cite: 781] (для GAV)
    federated_data = {
        "id": product_info["product_id"],
        "name": product_info["name"],
        "category": product_info["category"],
        "price": product_info["base_price"],
        "stock": None,
        "supplier": None,
        "location": None
    }

    if inventory_info:
        logger.info(f"Inventory data found for item_id: {product_id}")
        federated_data["stock"] = inventory_info.get("stock_quantity")
        federated_data["supplier"] = inventory_info.get("supplier")
        federated_data["location"] = inventory_info.get("warehouse_location")
    else:
        logger.info(f"No inventory data found for item_id: {product_id}. Some fields will be null.")

    # Валідація через Pydantic модель перед поверненням
    try:
        result = FederatedProduct(**federated_data)
        logger.info(f"Successfully federated data for product_id: {product_id}")
        return result
    except Exception as e: # Наприклад, pydantic.error_wrappers.ValidationError
        logger.error(f"Data validation error for federated product {product_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error validating federated product data")

# --- Додатковий ендпоінт для отримання всіх продуктів з основного джерела (для тестування) ---
class ProductSourceModel(BaseModel):
    product_id: int
    name: str
    category: str
    base_price: float

@app.get("/source/products", response_model=List[ProductSourceModel], tags=["Source Data"])
async def get_all_source_products():
    """Повертає всі продукти з основного джерела (products_source.json)."""
    try:
        with open(PRODUCTS_SOURCE_PATH, 'r', encoding='utf-8') as f:
            products = json.load(f)
        return [ProductSourceModel(**p) for p in products]
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"{PRODUCTS_SOURCE_PATH} not found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Запуск сервера та налаштування даних ---
@app.on_event("startup")
async def startup_event():
    """Створює тестові файли JSON при запуску, якщо вони не існують."""
    setup_mock_data()

# Щоб запустити цей сервер, збережіть код у файл (наприклад, lab_6.py)
# та виконайте в терміналі команду:
# uvicorn lab_6:app --reload
#
# Ендпоінти будуть доступні:
# GET /federated/products/{product_id} - для отримання федеративних даних про продукт
# GET /source/products - для перегляду даних з основного джерела
# Документація API (Swagger UI): http://127.0.0.1:8000/docs