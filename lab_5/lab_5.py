# etl_service.py

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
import json
import os
import logging

# --- Налаштування ---
# Шляхи до файлів джерел та цільового файлу
SOURCE_A_PATH = "source_products_branch_A.json"
SOURCE_B_PATH = "source_additional_info_branch_B.json"
CONSOLIDATED_DATA_PATH = "consolidated_electronics_store.json"

# --- Моделі даних (Pydantic) ---
class ProductBase(BaseModel):
    # Модель для основних атрибутів продукту
    name: str = Field(..., min_length=3)
    category: str
    price: float = Field(..., gt=0)
    stock: int = Field(..., ge=0)

class ProductSourceA(ProductBase):
    # Модель для продуктів з джерела А
    id: int

class ProductSourceB(BaseModel):
    # Модель для продуктів з джерела B (може мати інші поля)
    product_id: int # Може відповідати id з джерела А або бути новим
    name: Optional[str] = None # Опціонально, якщо оновлюємо існуючий
    category: Optional[str] = None # Опціонально
    description: Optional[str] = None
    supplier: Optional[str] = None
    old_price: Optional[float] = None # Для розрахунку зміни ціни
    price: Optional[float] = None # Нова ціна, якщо є
    stock: Optional[int] = None # Нова кількість, якщо є

class ConsolidatedProduct(ProductBase):
    # Фінальна модель консолідованого продукту
    id: int
    description: Optional[str] = None
    supplier: Optional[str] = None
    price_change_percentage: Optional[float] = None # Додане поле

# --- Ініціалізація FastAPI ---
app = FastAPI(
    title="ETL Service for Electronics Store",
    description="A web service to demonstrate an ETL process for consolidating product data.",
    version="1.0.0"
)

# --- Логіка ETL ---

def extract_data() -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Видобуває дані з файлів джерел.
    Повертає кортеж зі списками словників.
    """
    data_a = []
    data_b = []

    # Створюємо файли з тестовими даними, якщо вони не існують
    if not os.path.exists(SOURCE_A_PATH):
        sample_data_a = [
            {"id": 1, "name": "Laptop Alpha", "category": "Laptops", "price": 25000.00, "stock": 10},
            {"id": 2, "name": "Smartphone Beta", "category": "Smartphones", "price": 12000.00, "stock": 25},
            {"id": 3, "name": "Headphones Gamma", "category": "Audio", "price": 0, "stock": 5}, # "Погані" дані для очищення
            {"id": 4, "name": "Tablet Delta", "category": "Tablets", "price": 8500.00, "stock": 0}, # "Погані" дані для очищення
        ]
        with open(SOURCE_A_PATH, 'w', encoding='utf-8') as f:
            json.dump(sample_data_a, f, indent=4, ensure_ascii=False)
        logger.info(f"Created sample data file: {SOURCE_A_PATH}")

    if not os.path.exists(SOURCE_B_PATH):
        sample_data_b = [
            {"product_id": 1, "description": "High-performance laptop for professionals.", "supplier": "SupplierX", "old_price": 26000.00, "price": 24500.00}, # Оновлення існуючого
            {"product_id": 5, "name": "Smartwatch Epsilon", "category": "Wearables", "description": "Latest generation smartwatch.", "supplier": "SupplierY", "price": 7500.00, "stock": 30}, # Новий продукт
            {"product_id": 2, "supplier": "SupplierZ", "stock": 30} # Оновлення стоку і постачальника
        ]
        with open(SOURCE_B_PATH, 'w', encoding='utf-8') as f:
            json.dump(sample_data_b, f, indent=4, ensure_ascii=False)
        logger.info(f"Created sample data file: {SOURCE_B_PATH}")


    try:
        with open(SOURCE_A_PATH, 'r', encoding='utf-8') as f:
            data_a = json.load(f)
        logger.info(f"Data extracted successfully from {SOURCE_A_PATH}")
    except FileNotFoundError:
        logger.warning(f"Source file not found: {SOURCE_A_PATH}")
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {SOURCE_A_PATH}")

    try:
        with open(SOURCE_B_PATH, 'r', encoding='utf-8') as f:
            data_b = json.load(f)
        logger.info(f"Data extracted successfully from {SOURCE_B_PATH}")
    except FileNotFoundError:
        logger.warning(f"Source file not found: {SOURCE_B_PATH}")
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {SOURCE_B_PATH}")

    return data_a, data_b

def transform_data(data_a: List[Dict[str, Any]], data_b: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Трансформує видобуті дані.
    """
    transformed_products: Dict[int, ConsolidatedProduct] = {} # Використовуємо словник для легкого оновлення за ID

    # 1. Обробка даних з джерела А (очищення, початкове заповнення)
    for item_a_raw in data_a:
        try:
            # Валідація та очищення: видаляємо товари з ціною або кількістю 0
            if item_a_raw.get("price", 0) <= 0 or item_a_raw.get("stock", 0) <= 0:
                logger.info(f"Skipping product from source A due to zero price/stock: ID {item_a_raw.get('id')}")
                continue
            
            # Спроба валідувати через Pydantic модель
            item_a = ProductSourceA(**item_a_raw)

            # Стандартизація категорій (приклад)
            if item_a.category.lower() in ["laptops", "notebooks"]:
                item_a.category = "Ноутбуки"
            elif item_a.category.lower() in ["smartphones", "phones"]:
                item_a.category = "Смартфони"
            elif item_a.category.lower() in ["audio", "headphones"]:
                item_a.category = "Аудіотехніка"
            elif item_a.category.lower() in ["tablets"]:
                 item_a.category = "Планшети"
            elif item_a.category.lower() in ["wearables"]:
                 item_a.category = "Носимі пристрої"


            transformed_products[item_a.id] = ConsolidatedProduct(
                id=item_a.id,
                name=item_a.name,
                category=item_a.category,
                price=item_a.price,
                stock=item_a.stock
            )
        except Exception as e: # Загальний Exception для Pydantic ValidationError та інших
            logger.warning(f"Skipping invalid product data from source A: {item_a_raw}. Error: {e}")


    # 2. Обробка даних з джерела B (оновлення існуючих, додавання нових)
    current_max_id = max(transformed_products.keys()) if transformed_products else 0

    for item_b_raw in data_b:
        try:
            item_b = ProductSourceB(**item_b_raw) # Валідація через Pydantic
            product_id = item_b.product_id

            if product_id in transformed_products:
                # Оновлення існуючого продукту
                existing_product = transformed_products[product_id]
                logger.info(f"Updating existing product ID {product_id} with data from source B.")
                if item_b.name: existing_product.name = item_b.name
                if item_b.category: # Стандартизація категорій для оновлення
                    if item_b.category.lower() in ["laptops", "notebooks"]:
                        existing_product.category = "Ноутбуки"
                    elif item_b.category.lower() in ["smartphones", "phones"]:
                        existing_product.category = "Смартфони"
                    elif item_b.category.lower() in ["audio", "headphones"]:
                        existing_product.category = "Аудіотехніка"
                    elif item_b.category.lower() in ["tablets"]:
                        existing_product.category = "Планшети"
                    elif item_b.category.lower() in ["wearables"]:
                        existing_product.category = "Носимі пристрої"
                    else:
                        existing_product.category = item_b.category


                if item_b.description: existing_product.description = item_b.description
                if item_b.supplier: existing_product.supplier = item_b.supplier
                
                new_price_b = item_b.price if item_b.price is not None else existing_product.price
                old_price_for_calc = item_b.old_price if item_b.old_price is not None else existing_product.price
                
                if old_price_for_calc != 0 and new_price_b != old_price_for_calc : # Розрахунок зміни ціни
                    existing_product.price_change_percentage = round(((new_price_b - old_price_for_calc) / old_price_for_calc) * 100, 2)
                
                if item_b.price is not None: existing_product.price = item_b.price # Оновлюємо ціну, якщо вона вказана в B
                if item_b.stock is not None: existing_product.stock = item_b.stock

            else:
                # Додавання нового продукту з джерела B
                logger.info(f"Adding new product from source B with original ID {product_id}.")
                # Потрібно перевірити, чи всі обов'язкові поля є для ConsolidatedProduct
                if not all([item_b.name, item_b.category, item_b.price is not None, item_b.stock is not None]):
                    logger.warning(f"Skipping new product from source B due to missing core fields: {item_b_raw}")
                    continue
                
                current_max_id += 1 # Генеруємо новий ID для консолідованої бази
                new_id = current_max_id
                
                category_b = item_b.category
                # Стандартизація категорій для нових продуктів
                if category_b.lower() in ["laptops", "notebooks"]:
                    category_b_std = "Ноутбуки"
                elif category_b.lower() in ["smartphones", "phones"]:
                    category_b_std = "Смартфони"
                elif category_b.lower() in ["audio", "headphones"]:
                    category_b_std = "Аудіотехніка"
                elif category_b.lower() in ["tablets"]:
                     category_b_std = "Планшети"
                elif category_b.lower() in ["wearables"]:
                     category_b_std = "Носимі пристрої"
                else:
                    category_b_std = category_b


                transformed_products[new_id] = ConsolidatedProduct(
                    id=new_id, # Присвоюємо новий унікальний ID
                    name=item_b.name,
                    category=category_b_std,
                    price=item_b.price,
                    stock=item_b.stock,
                    description=item_b.description,
                    supplier=item_b.supplier
                )
        except Exception as e: # Загальний Exception для Pydantic ValidationError та інших
            logger.warning(f"Skipping invalid product data from source B: {item_b_raw}. Error: {e}")

    return [product.dict() for product in transformed_products.values()]


def load_data(data: List[Dict[str, Any]]):
    """
    Завантажує трансформовані дані у цільовий файл.
    """
    try:
        with open(CONSOLIDATED_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Data loaded successfully to {CONSOLIDATED_DATA_PATH}")
    except IOError:
        logger.error(f"Error writing data to {CONSOLIDATED_DATA_PATH}")

# --- Ендпоінти API ---

# Налаштування логера для виводу в консоль
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@app.post("/run-etl", status_code=status.HTTP_200_OK, summary="Run the full ETL process")
async def run_etl_process():
    """
    Запускає повний ETL процес: Видобування, Трансформація, Завантаження.
    """
    logger.info("ETL process started.")
    # Етап 1: Видобування
    data_a, data_b = extract_data()
    if not data_a and not data_b:
        logger.warning("No data extracted from sources. ETL process might not produce expected results.")
        # return {"message": "ETL process completed, but no data was extracted from sources."}


    # Етап 2: Трансформація
    transformed_data = transform_data(data_a, data_b)
    logger.info(f"Data transformation completed. {len(transformed_data)} products processed.")

    # Етап 3: Завантаження
    load_data(transformed_data)
    logger.info("ETL process finished successfully.")
    return {"message": "ETL process completed successfully.", "consolidated_items_count": len(transformed_data)}

@app.get("/consolidated-data", response_model=List[ConsolidatedProduct], summary="Get consolidated data")
async def get_consolidated_data():
    """
    Повертає консолідовані дані з цільового файлу.
    """
    try:
        with open(CONSOLIDATED_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Валідуємо кожен об'єкт даних перед поверненням
        validated_data = [ConsolidatedProduct(**item) for item in data]
        return validated_data
    except FileNotFoundError:
        logger.error(f"Consolidated data file not found: {CONSOLIDATED_DATA_PATH}. Run ETL first.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Consolidated data not found. Please run the ETL process first via POST /run-etl")
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from consolidated data file: {CONSOLIDATED_DATA_PATH}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error reading consolidated data.")
    except Exception as e: # Для Pydantic ValidationError та інших помилок
        logger.error(f"Error validating consolidated data: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error validating consolidated data: {str(e)}")


# --- Запуск сервера (для локальної розробки) ---
# Щоб запустити цей сервер, збережіть код у файл (наприклад, lab_5.py)
# та виконайте в терміналі команду:
# uvicorn lab_5:app --reload
#
# Ендпоінти будуть доступні:
# POST /run-etl - для запуску ETL процесу
# GET /consolidated-data - для перегляду консолідованих даних
# Документація API (Swagger UI): http://127.0.0.1:8000/docs
# Альтернативна документація (ReDoc): http://127.0.0.1:8000/redoc