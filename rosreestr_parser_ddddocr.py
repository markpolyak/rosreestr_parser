"""
Парсер справочной информации по объектам недвижимости (lk.rosreestr.ru)
Решение капчи — локально через ddddOCR (без внешних платных сервисов).

Установка зависимостей:
    pip install httpx easyocr pillow numpy

"""

import asyncio
import csv
import io
import json
from pathlib import Path

#import easyocr
import httpx
#import numpy as np
#from PIL import Image, ImageFilter, ImageOps
import ddddocr


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
    # пр. Славы 15, поиск по кадастровому номеру: curl 'https://nspd.gov.ru/api/geoportal/v2/search/geoportal?thematicSearchId=1&query=78%3A13%3A0741401%3A3018' -H 'referer: https://nspd.gov.ru/map?thematic=PKK&zoom=18&coordinate_x=3384781.855367066&coordinate_y=8361429.854958&theme_id=1&baseLayerId=235&is_copy_url=true' --insecure
    # пр. Славы 15, список помещений: curl 'https://nspd.gov.ru/api/geoportal/v1/tab-group-data?tabClass=objectsList&objdocId=3222814600&registersId=36441' --insecure

    # Вариант 1: явный список кадастровых номеров
    cad_numbers = [
        "78:13:0741401:2",
        "78:13:0741401:3018",
#        "78:13:0741401:3922",
    ]
    
    flats = ["78:13:0741401:3850","78:13:0741401:3851","78:13:0741401:3852","78:13:0741401:3853","78:13:0741401:3854","78:13:0741401:3855","78:13:0741401:3856","78:13:0741401:3857","78:13:0741401:3858","78:13:0741401:3859","78:13:0741401:3860","78:13:0741401:3861","78:13:0741401:3862","78:13:0741401:3863","78:13:0741401:3864","78:13:0741401:3865","78:13:0741401:3866","78:13:0741401:3867","78:13:0741401:3868","78:13:0741401:3869","78:13:0741401:3870","78:13:0741401:3871","78:13:0741401:3872","78:13:0741401:3873","78:13:0741401:3874","78:13:0741401:3875","78:13:0741401:3876","78:13:0741401:3877","78:13:0741401:3878","78:13:0741401:3879","78:13:0741401:3880","78:13:0741401:3881","78:13:0741401:3882","78:13:0741401:3883","78:13:0741401:3884","78:13:0741401:3885","78:13:0741401:3886","78:13:0741401:3887","78:13:0741401:3888","78:13:0741401:3889","78:13:0741401:3890","78:13:0741401:3891","78:13:0741401:3892","78:13:0741401:3893","78:13:0741401:3894","78:13:0741401:3895","78:13:0741401:3896","78:13:0741401:3897","78:13:0741401:3898","78:13:0741401:3899","78:13:0741401:3900","78:13:0741401:3901","78:13:0741401:3902","78:13:0741401:3903","78:13:0741401:3904","78:13:0741401:3905","78:13:0741401:3906","78:13:0741401:3907","78:13:0741401:3908","78:13:0741401:3909","78:13:0741401:3910","78:13:0741401:3911","78:13:0741401:3912","78:13:0741401:3913","78:13:0741401:3914","78:13:0741401:3915","78:13:0741401:3916","78:13:0741401:3917","78:13:0741401:3918","78:13:0741401:3919","78:13:0741401:3920","78:13:0741401:3921","78:13:0741401:3922","78:13:0741401:3923","78:13:0741401:3924","78:13:0741401:3925","78:13:0741401:3926","78:13:0741401:3927","78:13:0741401:3928","78:13:0741401:3929","78:13:0741401:3930","78:13:0741401:3931","78:13:0741401:3932","78:13:0741401:3933","78:13:0741401:3934","78:13:0741401:3935","78:13:0741401:3936","78:13:0741401:3937","78:13:0741401:3938","78:13:0741401:3939","78:13:0741401:3940","78:13:0741401:3941","78:13:0741401:3942","78:13:0741401:3943","78:13:0741401:3944","78:13:0741401:3945","78:13:0741401:3946","78:13:0741401:3947","78:13:0741401:3948","78:13:0741401:3949","78:13:0741401:3950","78:13:0741401:3951","78:13:0741401:3952","78:13:0741401:3953","78:13:0741401:3954","78:13:0741401:3955","78:13:0741401:3956","78:13:0741401:3957","78:13:0741401:3958","78:13:0741401:3959","78:13:0741401:3960","78:13:0741401:3961","78:13:0741401:3962","78:13:0741401:3963","78:13:0741401:3964","78:13:0741401:3965","78:13:0741401:3966","78:13:0741401:3967","78:13:0741401:3968","78:13:0741401:3969","78:13:0741401:3970","78:13:0741401:3971","78:13:0741401:3972","78:13:0741401:3973","78:13:0741401:3974","78:13:0741401:3975","78:13:0741401:3976","78:13:0741401:3977","78:13:0741401:3978","78:13:0741401:3979","78:13:0741401:3980","78:13:0741401:3981","78:13:0741401:3982","78:13:0741401:3983","78:13:0741401:3984","78:13:0741401:3985","78:13:0741401:3986","78:13:0741401:3987","78:13:0741401:3988","78:13:0741401:3989","78:13:0741401:3990","78:13:0741401:3991","78:13:0741401:3992","78:13:0741401:3993","78:13:0741401:3994","78:13:0741401:3995","78:13:0741401:3996","78:13:0741401:3997","78:13:0741401:3998","78:13:0741401:3999","78:13:0741401:4000","78:13:0741401:4001","78:13:0741401:4002","78:13:0741401:4003","78:13:0741401:4004","78:13:0741401:4005","78:13:0741401:4006","78:13:0741401:4007","78:13:0741401:4008","78:13:0741401:4009","78:13:0741401:4010","78:13:0741401:4011","78:13:0741401:4012","78:13:0741401:4013","78:13:0741401:4014","78:13:0741401:4015","78:13:0741401:4016","78:13:0741401:4017","78:13:0741401:4018","78:13:0741401:4019","78:13:0741401:4020","78:13:0741401:4021","78:13:0741401:4022","78:13:0741401:4023","78:13:0741401:4024","78:13:0741401:4025","78:13:0741401:4026","78:13:0741401:4027","78:13:0741401:4028","78:13:0741401:4029","78:13:0741401:4030","78:13:0741401:4031","78:13:0741401:4032","78:13:0741401:4033","78:13:0741401:4034","78:13:0741401:4035","78:13:0741401:4036","78:13:0741401:4037","78:13:0741401:4038","78:13:0741401:4039","78:13:0741401:4040","78:13:0741401:4041","78:13:0741401:4042","78:13:0741401:4043","78:13:0741401:4044","78:13:0741401:4045","78:13:0741401:4046","78:13:0741401:4047","78:13:0741401:4048"]
    
    cad_numbers += flats
    
    cad_numbers = ["78:13:0741401:4003", "78:13:0741401:3934"]

    # Вариант 2: диапазон (один дом, квартиры 1–100)
    # cad_numbers = cad_range("78:13:0007410", 1, 100)

    # Вариант 3: из текстового файла (одна строка = один номер)
    # cad_numbers = Path("input.txt").read_text().strip().splitlines()

    asyncio.run(run(cad_numbers, output_csv="results.csv"))
