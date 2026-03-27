# ---------------------------------------------------------
# SCRIPT DE SCRAPING DE PRECIOS
# Características:
#   - Scraping paralelo: un thread por proveedor
#   - Soporte Playwright para sitios con JS rendering
#   - Detección de URLs muertas (404 persistentes)
#   - Exportación de errores.csv y url_status.csv
# ---------------------------------------------------------

# 1. IMPORTACIONES
# ---------------------------------------------------------
import os
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging.handlers import RotatingFileHandler

import requests
import pandas as pd
from bs4 import BeautifulSoup
from tenacity import (
    retry, stop_after_attempt, wait_exponential, retry_if_exception_type
)

from utils import clean_price

# ---------------------------------------------------------
# 2. CONFIGURACIÓN DE LOGGING
# ---------------------------------------------------------
LOG_FILE = 'scrapper.log'

logger = logging.getLogger('price_scrapper')
logger.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ---------------------------------------------------------
# 3. CONSTANTES
# ---------------------------------------------------------
HISTORY_FILE      = 'precios_historicos.csv'
INPUT_FILE        = 'Scrappers.csv'
ERRORS_FILE       = 'errores.csv'
URL_STATUS_FILE   = 'url_status.csv'

# Número de 404s consecutivos para considerar una URL como muerta
DEAD_URL_THRESHOLD = 3

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    'Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    'Chrome/119.0.0.0 Safari/537.36',
]

# ---------------------------------------------------------
# 4. FETCH ESTÁTICO (requests + BeautifulSoup)
# ---------------------------------------------------------
@retry(
    retry=retry_if_exception_type((
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    )),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _get(url: str) -> requests.Response:
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    return requests.get(url, headers=headers, timeout=10)


def fetch_static(url: str, selector_nombre: str, selector_monto: str):
    """
    Descarga una página estática con requests y parsea con BeautifulSoup.
    Retorna (nombre, precio, status_code).
    """
    response = _get(url)
    if response.status_code != 200:
        return None, None, response.status_code

    soup = BeautifulSoup(response.content, 'html.parser')
    name_el  = soup.select_one(selector_nombre)
    price_el = soup.select_one(selector_monto)

    final_name = name_el.get_text(strip=True) if name_el else None

    final_price = None
    if price_el:
        raw = price_el.get_text(separator=',', strip=True).replace('\xa0', ' ')
        final_price = raw.replace('$,', '$').replace(' ,', ' ')

    return final_name, final_price, 200


# ---------------------------------------------------------
# 5. FETCH DINÁMICO (Playwright — para sitios con JS)
# ---------------------------------------------------------
def fetch_dynamic(url: str, selector_nombre: str, selector_monto: str, browser=None):
    """
    Descarga una página renderizada por JavaScript usando Playwright.
    Si se pasa un `browser` ya abierto, lo reutiliza (más eficiente).
    Retorna (nombre, precio, status_code).
    """
    own_pw = own_browser = False
    pw = None

    try:
        if browser is None:
            try:
                from playwright.sync_api import sync_playwright
            except ImportError:
                logger.warning("Playwright no instalado. Usando requests como fallback.")
                return fetch_static(url, selector_nombre, selector_monto)
            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=True)
            own_browser = True

        context = browser.new_context(user_agent=random.choice(USER_AGENTS))
        page = context.new_page()
        try:
            resp = page.goto(url, timeout=20_000, wait_until='networkidle')
            status_code = resp.status if resp else 0
            content = page.content()
        finally:
            context.close()

        if status_code != 200:
            return None, None, status_code

        soup = BeautifulSoup(content, 'html.parser')
        name_el  = soup.select_one(selector_nombre)
        price_el = soup.select_one(selector_monto)

        final_name = name_el.get_text(strip=True) if name_el else None

        final_price = None
        if price_el:
            raw = price_el.get_text(separator=',', strip=True).replace('\xa0', ' ')
            final_price = raw.replace('$,', '$').replace(' ,', ' ')

        return final_name, final_price, status_code

    finally:
        if own_browser and browser:
            browser.close()
        if pw:
            pw.stop()


# ---------------------------------------------------------
# 6. TRACKING DE URLs MUERTAS
# ---------------------------------------------------------
def load_url_status() -> dict:
    """
    Carga el historial de estado de URLs desde url_status.csv.
    Retorna dict: { url: {proveedor, codigo, consecutive_404s, ...} }
    """
    if os.path.exists(URL_STATUS_FILE):
        try:
            df = pd.read_csv(URL_STATUS_FILE, on_bad_lines='skip')
            return df.set_index('url').to_dict('index')
        except Exception as e:
            logger.warning(f"No se pudo leer url_status ({e}). Se empieza de cero.")
    return {}


def save_url_status(url_status: dict):
    """Persiste el estado de URLs en url_status.csv."""
    if not url_status:
        return
    df = pd.DataFrame.from_dict(url_status, orient='index').reset_index()
    df.rename(columns={'index': 'url'}, inplace=True)
    cols = ['url', 'proveedor', 'codigo', 'consecutive_404s', 'first_404_date', 'last_check_date']
    df = df.reindex(columns=cols)
    df.to_csv(URL_STATUS_FILE, index=False)


def update_url_status(url_status: dict, url: str, proveedor: str,
                      codigo: str, is_success: bool):
    """
    Actualiza el contador de 404 para una URL.
    - Éxito → resetea el contador.
    - Error → incrementa el contador y loga un WARNING si supera el umbral.
    """
    today = datetime.now().strftime('%Y-%m-%d')
    if url not in url_status:
        url_status[url] = {
            'proveedor':        proveedor,
            'codigo':           codigo,
            'consecutive_404s': 0,
            'first_404_date':   None,
            'last_check_date':  today,
        }

    entry = url_status[url]
    entry['last_check_date'] = today

    if is_success:
        entry['consecutive_404s'] = 0
        entry['first_404_date']   = None
    else:
        entry['consecutive_404s'] = entry.get('consecutive_404s', 0) + 1
        if entry['first_404_date'] is None:
            entry['first_404_date'] = today

        count = entry['consecutive_404s']
        if count >= DEAD_URL_THRESHOLD:
            logger.warning(
                f"[URL MUERTA ⚠️] {proveedor}-{codigo} lleva {count} errores consecutivos "
                f"desde {entry['first_404_date']}: {url}"
            )


# ---------------------------------------------------------
# 7. PROCESAMIENTO DE UNA FILA (thread-safe)
# ---------------------------------------------------------
def process_row(row: pd.Series, last_prices: dict, url_status: dict,
                playwright_browser=None) -> dict:
    """
    Scrapea una URL y retorna un dict con el resultado.
    Thread-safe: cada thread opera sobre claves únicas de su propio proveedor.

    Tipos de resultado: 'change' | 'equal' | 'skip' | 'error'
    """
    url            = row['url']
    proveedor      = row['proveedor']
    codigo         = str(row['cod'])
    codigo_interno = row['codigo_interno']
    method         = str(row.get('method', 'static')).strip().lower()
    timestamp      = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    base = {'proveedor': proveedor, 'codigo': codigo, 'url': url, 'timestamp': timestamp}

    try:
        # A. FETCH (estático o dinámico)
        if method == 'dynamic':
            final_name, final_price, status_code = fetch_dynamic(
                url, row['nombre'], row['monto'], browser=playwright_browser
            )
        else:
            final_name, final_price, status_code = fetch_static(
                url, row['nombre'], row['monto']
            )

        # B. MANEJAR ERRORES HTTP
        if status_code != 200:
            logger.error(f"[HTTP {status_code}] {proveedor}-{codigo} | {url}")
            update_url_status(url_status, url, proveedor, codigo, is_success=False)
            return {**base, 'type': 'error', 'error_type': f'HTTP {status_code}',
                    'nombre': final_name or '', 'precio': final_price or ''}

        # C. ÉXITO — resetear contador 404
        update_url_status(url_status, url, proveedor, codigo, is_success=True)

        final_name  = final_name  or "Nombre no encontrado"
        final_price = final_price or "Precio no encontrado"

        # D. VALIDACIONES
        is_valid_name  = final_name  != "Nombre no encontrado"
        is_valid_price = final_price != "Precio no encontrado"
        parsed_price   = clean_price(final_price)
        is_non_zero    = parsed_price is not None and parsed_price > 0

        if is_valid_name and is_valid_price and is_non_zero:
            key = (str(proveedor), str(codigo), str(url))
            last_known    = last_prices.get(key)
            last_parsed   = clean_price(last_known) if last_known else None
            price_changed = (last_parsed is None) or (abs(parsed_price - last_parsed) > 0.01)

            if price_changed:
                status_label = "NUEVO" if last_known is None else "CAMBIO"
                logger.info(f"[{status_label}] {proveedor}-{codigo}: {last_known} -> {final_price}")
                last_prices[key] = final_price  # GIL-safe: keys son exclusivas por proveedor
                return {
                    **base, 'type': 'change', 'status': status_label,
                    'data': {
                        'proveedor':        proveedor,
                        'codigo':           codigo,
                        'codigo_interno':   codigo_interno,
                        'nombre_detectado': final_name,
                        'precio_detectado': final_price,
                        'timestamp':        timestamp,
                        'url':              url,
                    }
                }
            else:
                logger.info(f"[IGUAL]  {proveedor}-{codigo}: Mantiene {final_price}")
                return {**base, 'type': 'equal'}
        else:
            logger.warning(
                f"[SKIP] {proveedor}-{codigo} | "
                f"nombre='{final_name}' | precio='{final_price}' | parsed={parsed_price}"
            )
            return {**base, 'type': 'skip', 'error_type': 'SKIP',
                    'nombre': final_name, 'precio': final_price}

    except Exception as e:
        logger.error(f"[FALLO] {proveedor}-{codigo} | {url}", exc_info=True)
        return {**base, 'type': 'error', 'error_type': f'EXCEPCION: {type(e).__name__}',
                'nombre': '', 'precio': ''}


# ---------------------------------------------------------
# 8. THREAD POR PROVEEDOR
# ---------------------------------------------------------
def scrape_provider(provider_name: str, rows_df: pd.DataFrame,
                    last_prices: dict, url_status: dict) -> list:
    """
    Procesa todos los productos de un proveedor secuencialmente con delays.
    Cada proveedor corre en su propio thread → paralelismo entre proveedores,
    respetando el rate limit de cada servidor.
    """
    logger.info(f"[THREAD START] {provider_name} — {len(rows_df)} URLs")

    # Inicializar Playwright si este proveedor tiene URLs dinámicas
    uses_playwright = (
        'method' in rows_df.columns and
        rows_df['method'].eq('dynamic').any()
    )
    pw = browser = None

    if uses_playwright:
        try:
            from playwright.sync_api import sync_playwright
            pw      = sync_playwright().start()
            browser = pw.chromium.launch(headless=True)
            logger.info(f"[PLAYWRIGHT] Browser iniciado para {provider_name}")
        except ImportError:
            logger.warning(f"[PLAYWRIGHT] No instalado — {provider_name} usará requests como fallback.")

    results = []
    try:
        for _, row in rows_df.iterrows():
            method = str(row.get('method', 'static')).strip().lower()
            result = process_row(
                row, last_prices, url_status,
                playwright_browser=browser if method == 'dynamic' else None
            )
            results.append(result)
            time.sleep(random.uniform(1.5, 3.5))
    finally:
        if browser:
            browser.close()
        if pw:
            pw.stop()

    logger.info(f"[THREAD END] {provider_name} — {len(results)} procesados")
    return results


# ---------------------------------------------------------
# 9. FUNCIÓN PRINCIPAL
# ---------------------------------------------------------
def main():
    run_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # --- Cargar historial de precios ---
    last_prices = {}
    if os.path.exists(HISTORY_FILE):
        try:
            df_history = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
            df_latest  = df_history.drop_duplicates(
                subset=['proveedor', 'codigo', 'url'], keep='last'
            )
            for _, row in df_latest.iterrows():
                key = (str(row['proveedor']), str(row['codigo']), str(row['url']))
                last_prices[key] = str(row['precio_detectado']).replace('\xa0', ' ').strip()
            logger.info(f"Historial cargado: {len(last_prices)} precios conocidos.")
        except Exception as e:
            logger.warning(f"No se pudo leer el historial ({e}). Se empieza de cero.")

    # --- Cargar estado de URLs ---
    url_status = load_url_status()
    logger.info(f"URL status cargado: {len(url_status)} URLs monitoreadas.")

    # --- Cargar CSV de configuración ---
    csv_file = pd.read_csv(INPUT_FILE)
    if 'method' not in csv_file.columns:
        csv_file['method'] = 'static'

    provider_groups = {p: g for p, g in csv_file.groupby('proveedor')}
    n_providers = len(provider_groups)

    logger.info(f"{'='*60}")
    logger.info(
        f"Iniciando escaneo PARALELO: {len(csv_file)} productos "
        f"en {n_providers} proveedores simultáneos"
    )
    logger.info(f"{'='*60}")

    # --- Scraping paralelo: un thread por proveedor ---
    all_results = []
    with ThreadPoolExecutor(max_workers=n_providers) as executor:
        futures = {
            executor.submit(scrape_provider, provider, group, last_prices, url_status): provider
            for provider, group in provider_groups.items()
        }
        for future in as_completed(futures):
            provider = futures[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                logger.error(f"[THREAD FALLO] Proveedor '{provider}': {e}", exc_info=True)

    # --- Agregar resultados ---
    new_data   = []
    error_data = []
    cnt_nuevos = cnt_cambios = cnt_igual = cnt_skip = cnt_error = 0

    for r in all_results:
        t = r.get('type')
        if t == 'change':
            new_data.append(r['data'])
            if r.get('status') == 'NUEVO':
                cnt_nuevos += 1
            else:
                cnt_cambios += 1
        elif t == 'equal':
            cnt_igual += 1
        elif t == 'skip':
            cnt_skip += 1
            error_data.append({
                'run_timestamp': run_timestamp,
                'proveedor':     r['proveedor'],
                'codigo':        r['codigo'],
                'url':           r['url'],
                'tipo_error':    r.get('error_type', 'SKIP'),
                'nombre':        r.get('nombre', ''),
                'precio':        r.get('precio', ''),
            })
        elif t == 'error':
            cnt_error += 1
            error_data.append({
                'run_timestamp': run_timestamp,
                'proveedor':     r['proveedor'],
                'codigo':        r['codigo'],
                'url':           r['url'],
                'tipo_error':    r.get('error_type', 'ERROR'),
                'nombre':        r.get('nombre', ''),
                'precio':        r.get('precio', ''),
            })

    # --- Guardar historial ---
    if new_data:
        df_new = pd.DataFrame(new_data)
        if os.path.exists(HISTORY_FILE):
            df_existing = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(
                subset=['proveedor', 'codigo', 'timestamp'], keep='last'
            )
            df_combined.to_csv(HISTORY_FILE, index=False)
        else:
            df_new.to_csv(HISTORY_FILE, index=False)
        logger.info(f"Se agregaron {len(new_data)} registros a '{HISTORY_FILE}'.")
    else:
        logger.info("No hubo variaciones de precio. El historial no se modificó.")

    # --- Guardar estado de URLs ---
    save_url_status(url_status)

    # --- Guardar errores ---
    if error_data:
        pd.DataFrame(error_data).to_csv(ERRORS_FILE, index=False)
        logger.info(f"Se exportaron {len(error_data)} entradas a '{ERRORS_FILE}'.")
    elif os.path.exists(ERRORS_FILE):
        os.remove(ERRORS_FILE)
        logger.info("Sin errores esta corrida. Se eliminó el archivo de errores anterior.")

    # --- Resumen final ---
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


# ---------------------------------------------------------
# 10. PUNTO DE ENTRADA
# ---------------------------------------------------------
if __name__ == "__main__":
    main()
