"""
Парсер справочной информации по объектам недвижимости (lk.rosreestr.ru)
Решение капчи — локально через ddddOCR (без внешних платных сервисов).

Установка зависимостей:
    pip install httpx easyocr pillow numpy

"""

import argparse
import asyncio
import csv
import io
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

#import easyocr
import httpx
#import numpy as np
#from PIL import Image, ImageFilter, ImageOps
import ddddocr


# ---------------------------------------------------------------------------
# Логгер (настраивается в точке входа через setup_logging)
# ---------------------------------------------------------------------------

log = logging.getLogger("rosreestr")


def setup_logging(silent: bool, log_file: str | None) -> None:
    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    if not silent:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        log.addHandler(fh)


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

# Инициализация (один раз при старте)
_ocr = ddddocr.DdddOcr(show_ad=False)

# ---------------------------------------------------------------------------
# Preprocessing + OCR капчи
# ---------------------------------------------------------------------------

def solve_captcha_local(image_bytes: bytes) -> str | None:
    result = _ocr.classification(image_bytes).lower().replace(" ", "")
    cleaned = "".join(c for c in result if c in CAPTCHA_CHARSET)
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
    max_retries: int = MAX_RETRIES,
) -> dict | None:
    for attempt in range(max_retries):
        try:
            image_bytes = await get_captcha_image(client)
            code = solve_captcha_local(image_bytes)

            if code is None:
                log.warning("OCR не распознал капчу (попытка %d), повтор...", attempt + 1)
                continue

            log.debug("Капча: '%s' (попытка %d)", code, attempt + 1)

            if not await validate_captcha(client, code):
                log.warning("Сервер отклонил капчу, повтор...")
                continue

            result = await fetch_object(client, cad_number, code)
            if result is not None:
                return result

            log.info("Объект не найден: %s", cad_number)
            return None

        except Exception as e:
            log.error("Ошибка (попытка %d): %s", attempt + 1, e)
            await asyncio.sleep(REQUEST_DELAY)

    log.error("Провал после %d попыток: %s", max_retries, cad_number)
    return None


# ---------------------------------------------------------------------------
# Справочники кодов
# ---------------------------------------------------------------------------

STATUS_CODES = {
    "0": "Ранее учтённый",
    "1": "Актуально",
    "5": "Снят с учёта",
}

OBJ_TYPE_CODES = {
    "002001001000": "Земельный участок",
    "002001002000": "Здание",
    "002001003000": "Помещение",
    "002001004000": "Сооружение",
    "002001005000": "Объект незавершённого строительства",
    "002001006000": "Машино-место",
    "002003004000": "Единый недвижимый комплекс",
}

PURPOSE_CODES = {
    "204001000000": "Нежилой",
    "204002000000": "Жилой",
    "204003000000": "Многоквартирный дом",
    "205001000000": "Линейный",
    "206001000000": "Нежилое",
    "206002000000": "Жилое",
}

# ---------------------------------------------------------------------------
# Утилита перевода Unix-timestamp (мс) в дату
# ---------------------------------------------------------------------------

def ts_to_date(ts_ms) -> str | None:
    if not ts_ms:
        return None
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%d.%m.%Y")


# ---------------------------------------------------------------------------
# Порядок полей в CSV — строго по README
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "cad_number", "cad_quarter", "status", "obj_type", "area", "floor", "purpose",
    "address_full", "address_region", "address_street", "address_house", "address_apartment",
    "reg_date", "cad_cost", "cad_cost_date", "rights", "encumbrances", "raw_json",
]


# ---------------------------------------------------------------------------
# Разворачиваем вложенный JSON в плоский словарь для CSV
# ---------------------------------------------------------------------------

def flatten_object(obj: dict) -> dict:
    addr = obj.get("address") or {}

    rights_parts = []
    for r in (obj.get("rights") or []):
        right_type = r.get("rightTypeDesc") or r.get("type") or ""
        right_num = r.get("rightNumber") or ""
        right_date = ts_to_date(r.get("rightRegDate"))
        part = r.get("part")
        parts = [right_type]
        if right_num:
            parts.append(f"№ {right_num}")
        if right_date:
            parts.append(f"от {right_date}")
        if part:
            parts.append(f"доля {part}")
        rights_parts.append(" ".join(p for p in parts if p))
    rights_str = "; ".join(rights_parts)

    enc_parts = []
    for e in (obj.get("encumbrances") or []):
        enc_type = e.get("typeDesc") or e.get("type") or ""
        enc_num = e.get("encumbranceNumber") or ""
        enc_date = ts_to_date(e.get("startDate"))
        parts = [enc_type]
        if enc_num:
            parts.append(f"№ {enc_num}")
        if enc_date:
            parts.append(f"от {enc_date}")
        enc_parts.append(" ".join(p for p in parts if p))
    enc_str = "; ".join(enc_parts)

    obj_type_raw = obj.get("objType")
    status_raw = obj.get("status")
    purpose_raw = obj.get("purpose")

    return {
        "cad_number":        obj.get("cadNumber"),
        "cad_quarter":       obj.get("cadQuarter"),
        "status":            STATUS_CODES.get(status_raw, status_raw),
        "obj_type":          OBJ_TYPE_CODES.get(obj_type_raw, obj_type_raw),
        "area":              obj.get("area"),
        "floor":             obj.get("floor") or obj.get("levelFloor"),
        "purpose":           PURPOSE_CODES.get(purpose_raw, purpose_raw),
        "address_full":      addr.get("readableAddress"),
        "address_region":    addr.get("region"),
        "address_street":    addr.get("street"),
        "address_house":     addr.get("house"),
        "address_apartment": addr.get("apartment"),
        "reg_date":          ts_to_date(obj.get("regDate")),
        "cad_cost":          obj.get("cadCost"),
        "cad_cost_date":     ts_to_date(obj.get("cadCostDeterminationDate")),
        "rights":            rights_str,
        "encumbrances":      enc_str,
        "raw_json":          json.dumps(obj, ensure_ascii=False),
    }


# ---------------------------------------------------------------------------
# Основной цикл
# ---------------------------------------------------------------------------

async def run(cad_numbers: list[str], output_csv: str = "results.csv", max_retries: int = MAX_RETRIES):
    results = []

    async with httpx.AsyncClient(verify=False, timeout=30) as client:
        for i, cad_number in enumerate(cad_numbers):
            log.info("[%d/%d] %s", i + 1, len(cad_numbers), cad_number)

            obj = await fetch_object_with_captcha(client, cad_number, max_retries=max_retries)
            if obj:
                row = flatten_object(obj)
                results.append(row)
                log.info("  ✓ %s | %s м²", row["address_full"], row["area"])
            else:
                results.append({"cad_number": cad_number, "error": "not_found"})
                log.warning("  ✗ не найден: %s", cad_number)

            if i < len(cad_numbers) - 1:
                await asyncio.sleep(REQUEST_DELAY)

    if not results:
        log.warning("Нет результатов")
        return

    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    ok = sum(1 for r in results if "error" not in r)
    log.info("Готово: %d/%d объектов → %s", ok, len(results), output_csv)


# ---------------------------------------------------------------------------
# Утилита: генерация диапазона кадастровых номеров
# ---------------------------------------------------------------------------

def cad_range(prefix: str, start: int, end: int) -> list[str]:
    """
    cad_range("50:20:0010203", 1, 100)
    → ["50:20:0010203:1", ..., "50:20:0010203:100"]
    """
    return [f"{prefix}:{n}" for n in range(start, end + 1)]


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Парсер справочной информации Росреестра по кадастровым номерам."
    )
    parser.add_argument(
        "-i", "--input",
        metavar="FILE",
        help="Текстовый файл с кадастровыми номерами (по одному на строке). "
             "Если не указан — используется список по умолчанию из скрипта.",
    )
    parser.add_argument(
        "-o", "--output",
        metavar="FILE",
        default="results.csv",
        help="Имя выходного CSV-файла (по умолчанию: results.csv).",
    )
    parser.add_argument(
        "-r", "--retries",
        metavar="N",
        type=int,
        default=MAX_RETRIES,
        help=f"Максимальное число попыток при ошибке капчи (по умолчанию: {MAX_RETRIES}).",
    )
    parser.add_argument(
        "-s", "--silent",
        action="store_true",
        help="Тихий режим: ничего не выводить в консоль.",
    )
    parser.add_argument(
        "-l", "--log",
        metavar="FILE",
        help="Файл для записи логов с таймстемпами (дополнительно к консоли, "
             "или вместо неё при -s).",
    )
    args = parser.parse_args()

    setup_logging(silent=args.silent, log_file=args.log)

    if args.input:
        cad_numbers = Path(args.input).read_text(encoding="utf-8").strip().splitlines()
        cad_numbers = [n.strip() for n in cad_numbers if n.strip()]
    else:
        # Список по умолчанию
        cad_numbers = [
            "50:20:0010203:1",
            "50:20:0010203:2",
            "50:20:0010203:100",
            "50:20:0010203:101",
        ]

    asyncio.run(run(cad_numbers, output_csv=args.output, max_retries=args.retries))
