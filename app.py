# ---------------------------------------------------------
# SCRIPT DE SCRAPING DE PRECIOS
# ---------------------------------------------------------

# 1. IMPORTACIÓN DE LIBRERÍAS
# ---------------------------------------------------------
import time
import random
import logging
import os

import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from logging.handlers import RotatingFileHandler
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from utils import clean_price

# ---------------------------------------------------------
# 2. CONFIGURACIÓN DE LOGGING
# ---------------------------------------------------------
LOG_FILE = 'scrapper.log'

logger = logging.getLogger('price_scrapper')
logger.setLevel(logging.DEBUG)

# Handler rotativo: máximo 5 MB por archivo, guarda hasta 3 backups
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

# Handler de consola (para verlo en GitHub Actions)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ---------------------------------------------------------
# 3. CONFIGURACIÓN INICIAL
# ---------------------------------------------------------
HISTORY_FILE = 'precios_historicos.csv'
INPUT_FILE = 'Scrappers.csv'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    'Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    'Chrome/119.0.0.0 Safari/537.36',
]

# ---------------------------------------------------------
# 4. FUNCIÓN DE FETCH CON REINTENTOS AUTOMÁTICOS
# ---------------------------------------------------------
@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                   requests.exceptions.Timeout,
                                   requests.exceptions.ChunkedEncodingError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def fetch_url(url: str) -> requests.Response:
    """
    Realiza un GET a la URL con un User-Agent aleatorio.
    Reintenta hasta 3 veces con backoff exponencial ante errores de red.
    """
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    response = requests.get(url, headers=headers, timeout=10)
    return response

# ---------------------------------------------------------
# 5. CARGA DEL HISTORIAL (MEMORIA)
# ---------------------------------------------------------
# Diccionario: { (proveedor, codigo, url) : precio_str }
last_prices = {}

if os.path.exists(HISTORY_FILE):
    try:
        df_history = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
        # Recorremos al revés para quedarnos con el dato más reciente por producto
        for _, row in df_history.iloc[::-1].iterrows():
            key = (str(row['proveedor']), str(row['codigo']), str(row['url']))
            if key not in last_prices:
                clean_p = str(row['precio_detectado']).replace('\xa0', ' ').strip()
                last_prices[key] = clean_p
        logger.info(f"Historial cargado: {len(last_prices)} precios conocidos.")
    except Exception as e:
        logger.warning(f"No se pudo leer el historial ({e}). Se empieza de cero.")

# ---------------------------------------------------------
# 6. BUCLE PRINCIPAL (EL SCRAPING)
# ---------------------------------------------------------
csv_file = pd.read_csv(INPUT_FILE)
new_data = []

# Contadores para el resumen final
cnt_igual = 0
cnt_skip = 0
cnt_error = 0

logger.info(f"{'='*60}")
logger.info(f"Iniciando escaneo de {len(csv_file)} productos...")
logger.info(f"{'='*60}")

for index, row in csv_file.iterrows():
    url = row['url']
    proveedor = row['proveedor']
    codigo = str(row['cod'])
    codigo_interno = row['codigo_interno']

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    final_name = "Nombre no encontrado"
    final_price = "Precio no encontrado"

    try:
        # A. PETICIÓN HTTP (con reintentos automáticos)
        response = fetch_url(url)

        if response.status_code != 200:
            logger.error(f"[HTTP {response.status_code}] {proveedor}-{codigo} | {url}")
            cnt_error += 1
            continue

        # B. PARSEO HTML
        soup = BeautifulSoup(response.content, 'html.parser')

        # C. EXTRACCIÓN DE DATOS
        name_element = soup.select_one(row['nombre'])
        if name_element:
            final_name = name_element.get_text(strip=True)

        price_element = soup.select_one(row['monto'])
        if price_element:
            raw_price = price_element.get_text(separator=',', strip=True).replace('\xa0', ' ')
            final_price = raw_price.replace('$,', '$').replace(' ,', ' ')

        # D. VALIDACIONES
        is_valid_name  = final_name  != "Nombre no encontrado"
        is_valid_price = final_price != "Precio no encontrado"
        parsed_price   = clean_price(final_price)           # float o None
        is_non_zero    = parsed_price is not None and parsed_price > 0

        if is_valid_name and is_valid_price and is_non_zero:
            key = (str(proveedor), str(codigo), str(url))
            last_known_price = last_prices.get(key)

            # Comparamos el precio limpio (float) actual vs el último conocido (float)
            last_parsed = clean_price(last_known_price) if last_known_price else None

            price_changed = (last_parsed is None) or (abs(parsed_price - last_parsed) > 0.01)

            if price_changed:
                status = "NUEVO" if last_known_price is None else "CAMBIO"
                logger.info(f"[{status}] {proveedor}-{codigo}: {last_known_price} -> {final_price}")

                new_data.append({
                    'proveedor':        proveedor,
                    'codigo':           codigo,
                    'codigo_interno':   codigo_interno,
                    'nombre_detectado': final_name,
                    'precio_detectado': final_price,
                    'timestamp':        timestamp,
                    'url':              url,
                    'status':           status,
                })
                last_prices[key] = final_price
            else:
                logger.info(f"[IGUAL]  {proveedor}-{codigo}: Mantiene {final_price}")
                cnt_igual += 1
        else:
            logger.warning(
                f"[SKIP] {proveedor}-{codigo} | "
                f"Datos incompletos -> nombre='{final_name}' | precio='{final_price}' | parsed={parsed_price}"
            )
            cnt_skip += 1

    except Exception as e:
        logger.error(f"[FALLO] {proveedor}-{codigo} | {url}", exc_info=True)
        cnt_error += 1

    # Delay aleatorio para no saturar los servidores
    time.sleep(random.uniform(1.5, 4.0))

# ---------------------------------------------------------
# 7. GUARDADO DE DATOS CON DEDUPLICACIÓN
# ---------------------------------------------------------
cnt_nuevos  = sum(1 for d in new_data if d.get('status') == 'NUEVO')
cnt_cambios = sum(1 for d in new_data if d.get('status') == 'CAMBIO')

if new_data:
    df_new = pd.DataFrame(new_data).drop(columns=['status'])  # 'status' es solo interno

    if os.path.exists(HISTORY_FILE):
        df_existing = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        # Eliminar duplicados exactos (misma clave + mismo timestamp)
        df_combined = df_combined.drop_duplicates(
            subset=['proveedor', 'codigo', 'timestamp'],
            keep='last'
        )
        df_combined.to_csv(HISTORY_FILE, index=False)
    else:
        df_new.to_csv(HISTORY_FILE, index=False)

    logger.info(f"Se agregaron {len(new_data)} registros a '{HISTORY_FILE}'.")
else:
    logger.info("No hubo variaciones de precio. El archivo no se ha tocado.")

# ---------------------------------------------------------
# 8. RESUMEN FINAL DE LA CORRIDA
# ---------------------------------------------------------
total = len(csv_file)
logger.info(f"{'='*60}")
logger.info(f"RESUMEN DE EJECUCIÓN")
logger.info(f"  Total procesados : {total}")
logger.info(f"  ✅ Nuevos        : {cnt_nuevos}")
logger.info(f"  🔄 Cambios       : {cnt_cambios}")
logger.info(f"  ➖ Sin cambios   : {cnt_igual}")
logger.info(f"  ⚠️  Incompletos  : {cnt_skip}")
logger.info(f"  ❌ Errores       : {cnt_error}")
logger.info(f"{'='*60}")
