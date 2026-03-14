"""
Парсер справочной информации по объектам недвижимости (lk.rosreestr.ru)
Решение капчи — локально через EasyOCR (без внешних платных сервисов).

Установка зависимостей:
    pip install httpx easyocr pillow numpy

При первом запуске EasyOCR скачает модели (~500 МБ).
"""

import asyncio
import csv
import io
import json
from pathlib import Path

import easyocr
import httpx
import numpy as np
from PIL import Image, ImageFilter, ImageOps

# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------

BASE_URL = "https://lk.rosreestr.ru/account-back"
REQUEST_DELAY = 3.0   # секунды между запросами объектов
MAX_RETRIES = 5       # попыток при неверной капче или ошибке сети
CAPTCHA_CHARSET = set("abcdefghijklmnopqrstuvwxyz0123456789")

# ---------------------------------------------------------------------------
# Заголовки
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
# OCR — инициализируем один раз при старте
# ---------------------------------------------------------------------------

print("Загружаем EasyOCR (первый раз займёт несколько минут)...")
_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
print("EasyOCR готов.")


# ---------------------------------------------------------------------------
# Preprocessing + OCR капчи
# ---------------------------------------------------------------------------

def preprocess_captcha(image_bytes: bytes) -> np.ndarray:
    """
    Подготавливаем изображение капчи:
    - grayscale
    - бинаризация (убирает линию-помеху и фон)
    - увеличение для лучшего распознавания мелких символов
    """
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    gray = ImageOps.grayscale(img)

    # Порог 160 хорошо отделяет текст от серого фона и линии
    binary = gray.point(lambda p: 255 if p > 160 else 0)
    inverted = ImageOps.invert(binary)

    w, h = inverted.size
    enlarged = inverted.resize((int(w * 2.5), int(h * 2.5)), Image.LANCZOS)
    sharpened = enlarged.filter(ImageFilter.SHARPEN)

    return np.array(sharpened)


def solve_captcha_local(image_bytes: bytes) -> str | None:
    """Распознаёт текст капчи. Возвращает 5-символьную строку или None."""
    img_array = preprocess_captcha(image_bytes)

    results = _reader.readtext(
        img_array,
        allowlist="abcdefghijklmnopqrstuvwxyz0123456789",
        detail=0,
        paragraph=False,
        width_ths=1.5,
        height_ths=1.5,
    )

    if not results:
        return None

    raw = "".join(results).lower().replace(" ", "")
    cleaned = "".join(c for c in raw if c in CAPTCHA_CHARSET)

    # Капча Росреестра всегда ровно 5 символов
    return cleaned if len(cleaned) == 5 else None


# ---------------------------------------------------------------------------
# HTTP-запросы к API
# ---------------------------------------------------------------------------

async def get_captcha_image(client: httpx.AsyncClient) -> bytes:
    resp = await client.get(f"{BASE_URL}/captcha.png", headers=HEADERS)
    resp.raise_for_status()
    return resp.content


async def validate_captcha(client: httpx.AsyncClient, code: str) -> bool:
    resp = await client.get(f"{BASE_URL}/captcha/{code}", headers=HEADERS)
    return resp.status_code == 200


async def search_by_address(client: httpx.AsyncClient, address: str) -> list[dict]:
    """Автокомплит адреса → список {cadnum, full_name}."""
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
    elements = resp.json().get("elements", [])
    return elements[0] if elements else None


# ---------------------------------------------------------------------------
# Получение объекта с автоматическим решением капчи и ретраями
# ---------------------------------------------------------------------------

async def fetch_object_with_captcha(
    client: httpx.AsyncClient,
    cad_number: str,
) -> dict | None:
    for attempt in range(MAX_RETRIES):
        try:
            image_bytes = await get_captcha_image(client)
            code = solve_captcha_local(image_bytes)

            if code is None:
                print(f"  OCR не распознал капчу (попытка {attempt + 1}), повтор...")
                continue

            print(f"  Капча: '{code}' (попытка {attempt + 1})")

            if not await validate_captcha(client, code):
                print(f"  Сервер отклонил капчу, повтор...")
                continue

            result = await fetch_object(client, cad_number, code)
            if result is not None:
                return result

            print(f"  Объект не найден: {cad_number}")
            return None

        except Exception as e:
            print(f"  Ошибка (попытка {attempt + 1}): {e}")
            await asyncio.sleep(REQUEST_DELAY)

    print(f"  Провал после {MAX_RETRIES} попыток: {cad_number}")
    return None


# ---------------------------------------------------------------------------
# Разворачиваем вложенный JSON в плоский словарь для CSV
# ---------------------------------------------------------------------------

def flatten_object(obj: dict) -> dict:
    addr = obj.get("address") or {}

    def ap(*keys):  # address part
        return " ".join(addr.get(k) or "" for k in keys).strip() or None

    full_address = ", ".join(p for p in [
        addr.get("region"),
        ap("cityType", "city"),
        ap("streetType", "street"),
        ap("houseType", "house"),
        ap("buildingType", "building"),
        ap("structureType", "structure"),
        ap("apartmentType", "apartment"),
    ] if p)

    rights_str = "; ".join(
        f"{r.get('type', '')} ({r.get('date', '')} №{r.get('number', '')})"
        for r in (obj.get("rights") or [])
    )
    enc_str = "; ".join(
        f"{e.get('type', '')} №{e.get('number', '')}"
        for e in (obj.get("encumbrances") or [])
    )

    return {
        "cad_number":        obj.get("cadNumber"),
        "cad_quarter":       obj.get("cadQuarter"),
        "status":            obj.get("status"),
        "obj_type":          obj.get("objType"),
        "area":              obj.get("area"),
        "floor":             obj.get("floor"),
        "purpose":           obj.get("purpose"),
        "address_full":      full_address,
        "address_region":    addr.get("region"),
        "address_street":    addr.get("street"),
        "address_house":     addr.get("house"),
        "address_apartment": addr.get("apartment"),
        "reg_date":          obj.get("regDate"),
        "cad_cost":          obj.get("cadCost"),
        "cad_cost_date":     obj.get("cadCostDate"),
        "rights":            rights_str,
        "encumbrances":      enc_str,
        "raw_json":          json.dumps(obj, ensure_ascii=False),
    }


# ---------------------------------------------------------------------------
# Основной цикл
# ---------------------------------------------------------------------------

async def run(cad_numbers: list[str], output_csv: str = "results.csv"):
    results = []

    async with httpx.AsyncClient(verify=False, timeout=30) as client:
        for i, cad_number in enumerate(cad_numbers):
            print(f"[{i+1}/{len(cad_numbers)}] {cad_number}")

            obj = await fetch_object_with_captcha(client, cad_number)
            if obj:
                row = flatten_object(obj)
                results.append(row)
                print(f"  ✓ {row['address_full']} | {row['area']} м²")
            else:
                results.append({"cad_number": cad_number, "error": "not_found"})
                print(f"  ✗ не найден")

            if i < len(cad_numbers) - 1:
                await asyncio.sleep(REQUEST_DELAY)

    if not results:
        print("Нет результатов")
        return

    all_keys: set[str] = set()
    for r in results:
        all_keys.update(r.keys())

    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=sorted(all_keys), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    ok = sum(1 for r in results if "error" not in r)
    print(f"\nГотово: {ok}/{len(results)} объектов → {output_csv}")


# ---------------------------------------------------------------------------
# Утилита: генерация диапазона кадастровых номеров
# ---------------------------------------------------------------------------

def cad_range(prefix: str, start: int, end: int) -> list[str]:
    """
    cad_range("78:13:0007410", 1, 100)
    → ["78:13:0007410:1", ..., "78:13:0007410:100"]
    """
    return [f"{prefix}:{n}" for n in range(start, end + 1)]


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Вариант 1: явный список кадастровых номеров
    cad_numbers = [
        "78:13:0741401:3922",
        "78:13:0741401:2",
    ]

    # Вариант 2: диапазон (один дом, квартиры 1–100)
    # cad_numbers = cad_range("78:13:0007410", 1, 100)

    # Вариант 3: из текстового файла (одна строка = один номер)
    # cad_numbers = Path("input.txt").read_text().strip().splitlines()

    asyncio.run(run(cad_numbers, output_csv="results.csv"))
