"""
Парсер справочной информации по объектам недвижимости (lk.rosreestr.ru)
Использует 2captcha для решения капчи.

Установка зависимостей:
    pip install httpx 2captcha-python Pillow

Получить API-ключ 2captcha: https://2captcha.com
"""

import asyncio
import csv
import json
import time
from pathlib import Path

import httpx
from twocaptcha import TwoCaptcha

# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------

TWOCAPTCHA_API_KEY = "ВАШ_КЛЮЧ_2CAPTCHA"

BASE_URL = "https://lk.rosreestr.ru/account-back"

# Задержка между запросами (секунды) — не спешим
REQUEST_DELAY = 3.0

# Количество повторных попыток при ошибке
MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Заголовки — имитируем браузер
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Referer": "https://lk.rosreestr.ru/eservices/real-estate-objects-online",
    "Origin": "https://lk.rosreestr.ru",
}


# ---------------------------------------------------------------------------
# Решение капчи
# ---------------------------------------------------------------------------

def solve_captcha(captcha_image_bytes: bytes, solver: TwoCaptcha) -> str:
    """Отправляем изображение в 2captcha, получаем текст."""
    # Сохраняем во временный файл (2captcha SDK требует путь или base64)
    tmp = Path("/tmp/captcha_rosreestr.png")
    tmp.write_bytes(captcha_image_bytes)
    result = solver.normal(str(tmp), caseSensitive=0)
    return result["code"]


# ---------------------------------------------------------------------------
# Основные функции запросов
# ---------------------------------------------------------------------------

async def get_captcha_image(client: httpx.AsyncClient) -> bytes:
    """GET /account-back/captcha.png — получить изображение капчи."""
    resp = await client.get(f"{BASE_URL}/captcha.png", headers=HEADERS)
    resp.raise_for_status()
    return resp.content


async def validate_captcha(client: httpx.AsyncClient, code: str) -> bool:
    """GET /account-back/captcha/{code} — проверить решение капчи."""
    resp = await client.get(f"{BASE_URL}/captcha/{code}", headers=HEADERS)
    return resp.status_code == 200


async def search_by_address(client: httpx.AsyncClient, address: str) -> list[dict]:
    """
    GET /account-back/address/search?term=...&objType=
    Возвращает список {cadnum, full_name}.
    """
    resp = await client.get(
        f"{BASE_URL}/address/search",
        params={"term": address, "objType": ""},
        headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


async def fetch_object(
    client: httpx.AsyncClient,
    cad_number: str,
    captcha_code: str,
) -> dict | None:
    """
    POST /account-back/on
    Возвращает первый элемент из ответа или None.
    """
    payload = {
        "filterType": "cadastral",
        "cadNumbers": [cad_number],
        "captcha": captcha_code,
    }
    resp = await client.post(
        f"{BASE_URL}/on",
        json=payload,
        headers={**HEADERS, "Content-Type": "application/json"},
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    elements = data.get("elements", [])
    return elements[0] if elements else None


# ---------------------------------------------------------------------------
# Получение объекта с капчей и ретраями
# ---------------------------------------------------------------------------

async def fetch_object_with_captcha(
    client: httpx.AsyncClient,
    cad_number: str,
    solver: TwoCaptcha,
) -> dict | None:
    for attempt in range(MAX_RETRIES):
        try:
            # 1. Получаем капчу
            image_bytes = await get_captcha_image(client)

            # 2. Решаем через 2captcha
            print(f"  Решаем капчу для {cad_number} (попытка {attempt + 1})...")
            code = solve_captcha(image_bytes, solver)
            print(f"  Капча: {code}")

            # 3. Валидируем
            if not await validate_captcha(client, code):
                print(f"  Капча отклонена сервером, повтор...")
                continue

            # 4. Запрос объекта
            result = await fetch_object(client, cad_number, code)
            if result is not None:
                return result

            print(f"  Объект не найден для {cad_number}")
            return None

        except Exception as e:
            print(f"  Ошибка (попытка {attempt + 1}): {e}")
            await asyncio.sleep(REQUEST_DELAY)

    print(f"  Не удалось получить данные для {cad_number} после {MAX_RETRIES} попыток")
    return None


# ---------------------------------------------------------------------------
# Парсинг ответа в плоский словарь для CSV
# ---------------------------------------------------------------------------

def flatten_object(obj: dict) -> dict:
    """Разворачивает вложенный JSON-ответ в плоский словарь."""
    addr = obj.get("address") or {}

    # Собираем читаемый адрес
    parts = [
        addr.get("region"),
        f"{addr.get('cityType', '')} {addr.get('city', '')}".strip() or None,
        f"{addr.get('streetType', '')} {addr.get('street', '')}".strip() or None,
        f"{addr.get('houseType', '')} {addr.get('house', '')}".strip() or None,
        f"{addr.get('buildingType', '')} {addr.get('building', '')}".strip() or None,
        f"{addr.get('apartmentType', '')} {addr.get('apartment', '')}".strip() or None,
    ]
    full_address = ", ".join(p for p in parts if p)

    # Права и обременения — в строку
    rights = obj.get("rights") or []
    rights_str = "; ".join(
        f"{r.get('type', '')} ({r.get('date', '')} №{r.get('number', '')})"
        for r in rights
    )
    encumbrances = obj.get("encumbrances") or []
    enc_str = "; ".join(
        f"{e.get('type', '')} №{e.get('number', '')}"
        for e in encumbrances
    )

    return {
        "cad_number": obj.get("cadNumber"),
        "cad_quarter": obj.get("cadQuarter"),
        "status": obj.get("status"),
        "obj_type": obj.get("objType"),
        "area": obj.get("area"),
        "floor": obj.get("floor"),
        "purpose": obj.get("purpose"),
        "address_full": full_address,
        "address_region": addr.get("region"),
        "address_street": addr.get("street"),
        "address_house": addr.get("house"),
        "address_apartment": addr.get("apartment"),
        "reg_date": obj.get("regDate"),
        "cad_cost": obj.get("cadCost"),
        "cad_cost_date": obj.get("cadCostDate"),
        "rights": rights_str,
        "encumbrances": enc_str,
        "raw_json": json.dumps(obj, ensure_ascii=False),  # полный ответ на случай новых полей
    }


# ---------------------------------------------------------------------------
# Основной цикл
# ---------------------------------------------------------------------------

async def run(
    cad_numbers: list[str],
    output_csv: str = "results.csv",
):
    solver = TwoCaptcha(TWOCAPTCHA_API_KEY)
    results = []

    async with httpx.AsyncClient(verify=False, timeout=30) as client:
        for i, cad_number in enumerate(cad_numbers):
            print(f"[{i+1}/{len(cad_numbers)}] {cad_number}")

            obj = await fetch_object_with_captcha(client, cad_number, solver)
            if obj:
                row = flatten_object(obj)
                results.append(row)
                print(f"  OK: {row['address_full']}, площадь {row['area']} м²")
            else:
                results.append({"cad_number": cad_number, "error": "not_found"})

            # Задержка между объектами
            if i < len(cad_numbers) - 1:
                await asyncio.sleep(REQUEST_DELAY)

    # Сохраняем CSV
    if results:
        fieldnames = list(results[0].keys())
        # Объединяем все ключи (на случай разных полей у разных объектов)
        all_keys: set[str] = set()
        for r in results:
            all_keys.update(r.keys())
        fieldnames = sorted(all_keys)

        with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(results)

        print(f"\nГотово: {len(results)} объектов сохранено в {output_csv}")
    else:
        print("Нет результатов")


# ---------------------------------------------------------------------------
# Вспомогательная функция: диапазон кадастровых номеров
# ---------------------------------------------------------------------------

def cad_range(prefix: str, start: int, end: int) -> list[str]:
    """
    Генерирует список кадастровых номеров вида prefix:start..end
    Пример: cad_range("78:13:0007410", 1, 100)
    → ["78:13:0007410:1", "78:13:0007410:2", ...]
    """
    return [f"{prefix}:{n}" for n in range(start, end + 1)]


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # --- Вариант 1: список кадастровых номеров напрямую ---
    cad_numbers = [
        "78:13:0007410:1",
        "78:13:0007410:2",
        # ...
    ]

    # --- Вариант 2: диапазон (один дом, квартиры 1–50) ---
    # cad_numbers = cad_range("78:13:0007410", 1, 50)

    # --- Вариант 3: загрузка из файла (одна строка = один номер) ---
    # cad_numbers = Path("input.txt").read_text().strip().splitlines()

    asyncio.run(run(cad_numbers, output_csv="results.csv"))
