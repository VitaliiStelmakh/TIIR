# Імпортуємо необхідні бібліотеки
import logging
from zeep import Client
from zeep.exceptions import Fault, ValidationError, TransportError

# Налаштування логування
# Рівень INFO, формат: Час - Назва логера - Рівень - Повідомлення
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# Створюємо об'єкт логера для цього модуля
logger = logging.getLogger("TemperatureWSClient")

# URL WSDL вебсервісу
WSDL_URL = 'https://webservices.daehosting.com/services/TemperatureConversions.wso?wsdl'


def create_client(wsdl_url=WSDL_URL):
    """
    Створює та повертає SOAP клієнт для вказаного WSDL URL.

    Args:
        wsdl_url (str): URL WSDL-файлу вебсервісу.

    Returns:
        zeep.Client | None: Об'єкт SOAP клієнта або None у разі помилки.
    """
    try:
        # Спроба створити клієнт
        client = Client(wsdl=wsdl_url)
        logger.info(f"SOAP client created successfully for {wsdl_url}")
        return client
    except Exception as e:
        # Логування помилки при створенні клієнта
        logger.error(f"Failed to create SOAP client: {e}", exc_info=True)
        return None


def safe_soap_call(operation_name, service_method, *args, **kwargs): # Додано operation_name
    """
    Безпечно викликає метод SOAP сервісу з обробкою типових помилок.

    Args:
        operation_name (str): Назва операції для логування.
        service_method (callable): Метод сервісу zeep для виклику.
        *args: Позиційні аргументи для методу сервісу.
        **kwargs: Іменовані аргументи для методу сервісу.

    Returns:
        Any | None: Результат виклику методу сервісу або None у разі помилки.
    """
    try:
        # Виклик переданого методу сервісу
        logger.info(f"Calling SOAP operation: {operation_name}") # Використовуємо operation_name
        response = service_method(*args, **kwargs)
        logger.info(f"SOAP call successful: {operation_name}")
        return response
    except Fault as e:
        # Обробка помилок SOAP Fault (помилки на стороні сервера)
        logger.error(f"SOAP Fault during {operation_name}: {e.message}", exc_info=False)
    except ValidationError as e:
        # Обробка помилок валідації даних (невідповідність типів)
        logger.error(f"Validation error during {operation_name}: {e}", exc_info=True)
    except TransportError as e:
        # Обробка помилок транспортного рівня (проблеми з мережею, URL)
        logger.error(f"Transport error during {operation_name}: {e}", exc_info=True)
    except Exception as e:
        # Обробка інших неочікуваних помилок
        logger.error(f"Unexpected error during {operation_name}: {e}", exc_info=True)
    return None


def convert_celsius_to_fahrenheit(client, celsius_temp):
    """
    Конвертує температуру з Цельсія у Фаренгейт за допомогою вебсервісу.

    Args:
        client (zeep.Client): Ініціалізований SOAP клієнт.
        celsius_temp (any): Температура в градусах Цельсія (може бути числом або рядком).

    Returns:
        float | None: Температура у Фаренгейтах або None у разі помилки.
    """
    operation_name = "CelsiusToFahrenheit" # Визначаємо назву операції
    try:
        # Конвертація вхідного значення у float
        n_celsius = float(celsius_temp)
        # Безпечний виклик методу сервісу, передаємо назву операції
        response = safe_soap_call(operation_name, client.service.CelsiusToFahrenheit, nCelsius=n_celsius)
        # Повернення результату як float, якщо виклик успішний
        return float(response) if response is not None else None
    except ValueError:
        # Обробка помилки, якщо вхідне значення не можна конвертувати у float
        logger.error(f"Invalid input for {operation_name}: '{celsius_temp}'. Must be a number.")
        return None


def convert_fahrenheit_to_celsius(client, fahrenheit_temp):
    """
    Конвертує температуру з Фаренгейта у Цельсій за допомогою вебсервісу.

    Args:
        client (zeep.Client): Ініціалізований SOAP клієнт.
        fahrenheit_temp (any): Температура в градусах Фаренгейта.

    Returns:
        float | None: Температура у Цельсіях або None у разі помилки.
    """
    operation_name = "FahrenheitToCelsius"
    try:
        n_fahrenheit = float(fahrenheit_temp)
        response = safe_soap_call(operation_name, client.service.FahrenheitToCelsius, nFahrenheit=n_fahrenheit)
        return float(response) if response is not None else None
    except ValueError:
        logger.error(f"Invalid input for {operation_name}: '{fahrenheit_temp}'. Must be a number.")
        return None


def calculate_wind_chill_celsius(client, celsius_temp, wind_speed):
    """
    Розраховує температуру охолодження вітром у Цельсіях.

    Args:
        client (zeep.Client): Ініціалізований SOAP клієнт.
        celsius_temp (any): Температура в градусах Цельсія.
        wind_speed (any): Швидкість вітру (в км/год, як очікує сервіс).

    Returns:
        float | None: Розрахована температура охолодження вітром або None у разі помилки.
    """
    operation_name = "WindChillInCelsius"
    try:
        n_celsius = float(celsius_temp)
        n_wind_speed = float(wind_speed)
        response = safe_soap_call(
            operation_name,
            client.service.WindChillInCelsius,
            nCelsius=n_celsius,
            nWindSpeed=n_wind_speed
        )
        return float(response) if response is not None else None
    except ValueError:
        logger.error(f"Invalid numeric input for {operation_name}: celsius='{celsius_temp}', wind_speed='{wind_speed}'")
        return None


def calculate_wind_chill_fahrenheit(client, fahrenheit_temp, wind_speed):
    """
    Розраховує температуру охолодження вітром у Фаренгейтах.

    Args:
        client (zeep.Client): Ініціалізований SOAP клієнт.
        fahrenheit_temp (any): Температура в градусах Фаренгейта.
        wind_speed (any): Швидкість вітру (в милях/год, як очікує сервіс).

    Returns:
        float | None: Розрахована температура охолодження вітром або None у разі помилки.
    """
    operation_name = "WindChillInFahrenheit"
    try:
        n_fahrenheit = float(fahrenheit_temp)
        n_wind_speed = float(wind_speed)
        response = safe_soap_call(
            operation_name,
            client.service.WindChillInFahrenheit,
            nFahrenheit=n_fahrenheit,
            nWindSpeed=n_wind_speed
        )
        return float(response) if response is not None else None
    except ValueError:
        logger.error(f"Invalid numeric input for {operation_name}: fahrenheit='{fahrenheit_temp}', wind_speed='{wind_speed}'")
        return None


def run_demo():
    """
    Запускає демонстраційні виклики всіх операцій вебсервісу.
    """
    client = create_client()
    if client is None:
        print("SOAP client initialization failed. Cannot run demo.")
        return

    print("\n--- Testing CelsiusToFahrenheit ---")
    for celsius_value in [0, 100, -40]:
        result = convert_celsius_to_fahrenheit(client, celsius_value)
        if result is not None:
            print(f"Input: {celsius_value}°C -> Output: {result:.2f}°F")

    print("\n--- Testing FahrenheitToCelsius ---")
    for fahrenheit_value in [32, 212, -40]:
        result = convert_fahrenheit_to_celsius(client, fahrenheit_value)
        if result is not None:
            print(f"Input: {fahrenheit_value}°F -> Output: {result:.2f}°C")

    print("\n--- Testing WindChillInCelsius ---")
    test_cases_celsius = [(5, 20), (-10, 30), (0, 5)]
    for temp, speed in test_cases_celsius:
        result = calculate_wind_chill_celsius(client, temp, speed)
        if result is not None:
            print(f"Input: Temp={temp}°C, Wind={speed} km/h -> Output: {result:.2f}°C")

    print("\n--- Testing WindChillInFahrenheit ---")
    test_cases_fahrenheit = [(41, 15), (14, 25), (32, 5)] # 41F=5C, 14F=-10C, 32F=0C
    for temp, speed in test_cases_fahrenheit:
        result = calculate_wind_chill_fahrenheit(client, temp, speed)
        if result is not None:
            print(f"Input: Temp={temp}°F, Wind={speed} mph -> Output: {result:.2f}°F")

    print("\n--- Testing Error Handling (Invalid Input Type) ---")
    # Спроба передати нечислове значення
    convert_celsius_to_fahrenheit(client, "invalid_temperature")

if __name__ == "__main__":
    logger.info("--- Starting TemperatureConversions web service client demo ---")
    run_demo()
    logger.info("--- Demo finished ---")

