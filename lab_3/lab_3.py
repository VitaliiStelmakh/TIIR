#(Консольний клієнт для ЛР3)
import requests
import json
# Базовий URL API 
BASE_URL = "http://127.0.0.1:8000"

def print_product(product):
    """Допоміжна функція для гарного виводу інформації про продукт."""
    print("-" * 20)
    print(f"ID: {product.get('id')}")
    print(f"Name: {product.get('name')}")
    print(f"Category: {product.get('category')}")
    print(f"Price: {product.get('price'):.2f} UAH")
    print(f"In stock: {product.get('stock')} pcs.")
    print("-" * 20)

def get_all_products():
    """Отримує та виводить список всіх продуктів."""
    try:
        response = requests.get(f"{BASE_URL}/products")
        response.raise_for_status() # Перевірка на HTTP помилки (4xx, 5xx)
        products = response.json()
        if not products:
            print("Product catalog is empty.")
            return
        print("\n--- Product List ---")
        for product in products:
            print_product(product)
    except requests.exceptions.RequestException as e:
        print(f"Network or API error: {e}")
    except json.JSONDecodeError:
        print("Error: Could not parse server response.")

def get_product_by_id():
    """Запитує ID та виводить інформацію про конкретний продукт."""
    try:
        product_id = int(input("Enter product ID: "))
        response = requests.get(f"{BASE_URL}/products/{product_id}")
        if response.status_code == 404:
            print(f"Error: Product with ID {product_id} not found.")
        elif response.status_code == 200:
            product = response.json()
            print("\n--- Product Information ---")
            print_product(product)
        else:
            response.raise_for_status() # Викликає помилку для інших статусів
    except ValueError:
        print("Error: Product ID must be an integer.")
    except requests.exceptions.RequestException as e:
        print(f"Network or API error: {e}")
    except json.JSONDecodeError:
        print("Error: Could not parse server response.")

def add_product():
    """Запитує дані та додає новий продукт."""
    print("\n--- Add New Product ---")
    try:
        name = input("Product name: ")
        category = input("Category: ")
        price_str = input("Price (UAH): ")
        stock_str = input("Quantity in stock: ")
        # Валідація введених даних
        if not name or not category:
            print("Error: Name and category cannot be empty.")
            return
        price = float(price_str)
        stock = int(stock_str)
        if price <= 0 or stock < 0:
            print("Error: Price must be positive, stock cannot be negative.")
            return
        product_data = {
            "name": name,
            "category": category,
            "price": price,
            "stock": stock
        }
        response = requests.post(f"{BASE_URL}/products", json=product_data)
        response.raise_for_status()
        if response.status_code == 201:
            new_product = response.json()
            print("\nProduct has been added successfully:")
            print_product(new_product)
        else:
            print(f"Unexpected response status: {response.status_code}")
    except ValueError:
        print("Error: Price must be a number, quantity must be an integer.")
    except requests.exceptions.RequestException as e:
        print(f"Network or API error: {e}")
    except json.JSONDecodeError:
        print("Error: Could not parse server response.")

def update_existing_product():
    """Запитує ID та нові дані для оновлення продукту."""
    print("\n--- Update Product ---")
    try:
        product_id = int(input("Enter product ID to update: "))
        # Спочатку перевіримо, чи існує товар
        get_response = requests.get(f"{BASE_URL}/products/{product_id}")
        if get_response.status_code == 404:
            print(f"Error: Product with ID {product_id} not found.")
            return
        get_response.raise_for_status()
        print("Enter new data (leave blank to keep unchanged):")
        name = input(f"New name: ")
        category = input(f"New category: ")
        price_str = input(f"New price (UAH): ")
        stock_str = input(f"New stock quantity: ")
        update_data = {}
        if name:
            update_data["name"] = name
        if category:
            update_data["category"] = category
        if price_str:
            price = float(price_str)
            if price <= 0: raise ValueError("Price must be positive")
            update_data["price"] = price
        if stock_str:
            stock = int(stock_str)
            if stock < 0: raise ValueError("Stock cannot be negative")
            update_data["stock"] = stock
        if not update_data:
            print("No data to update.")
            return
        response = requests.put(f"{BASE_URL}/products/{product_id}", json=update_data)
        response.raise_for_status()
        if response.status_code == 200:
            updated_product = response.json()
            print("\nProduct has been updated successfully:")
            print_product(updated_product)
        else:
            print(f"Unexpected response status: {response.status_code}")
    except ValueError as ve:
        print(f"Input error: {ve}")
    except requests.exceptions.RequestException as e:
        print(f"Network or API error: {e}")
    except json.JSONDecodeError:
        print("Error: Could not parse server response.")

def delete_existing_product():
    """Запитує ID та видаляє продукт."""
    print("\n--- Delete Product ---")
    try:
        product_id = int(input("Enter product ID to delete: "))
        response = requests.delete(f"{BASE_URL}/products/{product_id}")
        if response.status_code == 204:
            print(f"Product with ID {product_id} was successfully deleted.")
        elif response.status_code == 404:
            print(f"Error: Product with ID {product_id} not found.")
        else:
            response.raise_for_status() # Викликає помилку для інших статусів
    except ValueError:
        print("Error: Product ID must be an integer.")
    except requests.exceptions.RequestException as e:
        print(f"Network or API error: {e}")

def print_menu():
    """Виводить меню опцій."""
    print("\n--- Electronics Shop Client Menu ---")
    print("1. Show all products")
    print("2. Find product by ID")
    print("3. Add new product")
    print("4. Update product")
    print("5. Delete product")
    print("0. Exit")
    print("-" * 41)

# --- Головний цикл програми ---
if __name__ == "__main__":
    while True:
        print_menu()
        choice = input("Your choice: ")
        if choice == '1':
            get_all_products()
        elif choice == '2':
            get_product_by_id()
        elif choice == '3':
            add_product()
        elif choice == '4':
            update_existing_product()
        elif choice == '5':
            delete_existing_product()
        elif choice == '0':
            print("Exiting client.")
            break
        else:
            print("Invalid choice. Please try again.")
        input("\nPress Enter to continue...") # Пауза для перегляду результату