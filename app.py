# ---------------------------------------------------------
# SCRIPT DE SCRAPING DE PRECIOS - GUÍA DE APRENDIZAJE
# ---------------------------------------------------------

# 1. IMPORTACIÓN DE LIBRERÍAS
# ---------------------------------------------------------
import requests             # "El Navegador": Permite hacer peticiones a páginas web (simula poner una URL y dar enter).
from bs4 import BeautifulSoup # "El Traductor": Toma el código HTML feo de la web y lo convierte en objetos Python fáciles de leer.
import pandas as pd         # "El Excel": Maneja tablas de datos, lee y escribe CSVs de forma eficiente.
from datetime import datetime # "El Reloj": Utiles para saber qué hora es (timestamps).
import os                   # "El Sistema Operativo": Permite chequear si existen archivos, rutas, etc.

# ---------------------------------------------------------
# 2. CONFIGURACIÓN INICIAL
# ---------------------------------------------------------
HISTORY_FILE = 'precios_historicos.csv'  # Donde guardaremos el historial de cambios.
INPUT_FILE = 'Scrappers.csv'             # De donde sacamos qué buscar.
LOG_FILE = 'errores.csv'                 # Donde anotamos si algo sale mal (en formato CSV).

# Función auxiliar para guardar errores en un archivo CSV
def log_error(proveedor, codigo, url, motivo, detalle=""):
    # Preparamos los datos
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Comprobamos si el archivo existe para poner cabecera
    file_exists = os.path.exists(LOG_FILE)
    
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        # Si es nuevo, escribimos los títulos de las columnas
        if not file_exists:
            f.write("timestamp,proveedor,codigo,url,motivo,detalle_tecnico\n")
            
        # Limpiamos un poco los textos para no romper el CSV (quitamos comas y saltos de línea)
        clean_motivo = str(motivo).replace(',', ';').replace('\n', ' ')
        clean_detalle = str(detalle).replace(',', ';').replace('\n', ' ')
        
        # Escribimos la línea
        f.write(f"{timestamp},{proveedor},{codigo},{url},{clean_motivo},{clean_detalle}\n")

# ---------------------------------------------------------
# 3. CARGA DEL HISTORIAL (MEMORIA)
# ---------------------------------------------------------
# Necesitamos saber el "último precio conocido" para no guardar datos repetidos.
last_prices = {} # Diccionario: { (proveedor, codigo, url) : precio }

if os.path.exists(HISTORY_FILE):
    try:
        # Leemos el CSV. on_bad_lines='skip' evita que el script explote si hay una línea mal formada.
        df_history = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
        
        # Recorremos el historial al revés (iloc[::-1]) para quedarnos con el dato más reciente de cada producto.
        # Recorremos el historial al revés (iloc[::-1]) para quedarnos con el dato más reciente de cada producto.
        for _, row in df_history.iloc[::-1].iterrows(): 
            # Creamos una "clave única" para identificar al producto.
            # Usamos STR(codigo) para evitar problemas si uno lo lee como número y otro como texto.
            key = (str(row['proveedor']), str(row['codigo']), str(row['url']))
            
            # Si no hemos guardado este producto en memoria todavía, lo guardamos.
            if key not in last_prices:
                # Normalizamos precio: quitamos espacios raros (\xa0) y externos
                clean_price = str(row['precio_detectado']).replace('\xa0', ' ').strip()
                last_prices[key] = clean_price
                
    except Exception as e:
        print(f"Nota: Hubo un problema leyendo el historial ({e}). Se empezará de cero.")

# ---------------------------------------------------------
# 4. BUCLE PRINCIPAL (EL SCRAPING)
# ---------------------------------------------------------
csv_file = pd.read_csv(INPUT_FILE)
new_data = [] # Aquí iremos juntando solo las novedades para guardar al final.

print(f"Iniciando escaneo de {len(csv_file)} productos...")

for index, row in csv_file.iterrows():
    # Extraemos los datos de la fila actual del CSV de entrada
    url = row['url']
    proveedor = row['proveedor']
    codigo = row['cod']
    codigo_interno = row['codigo_interno'] # NUEVO: Código unificado para comparar proveedores
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Valores por defecto por si falla el scraping
    final_name = "Nombre no encontrado"
    final_price = "Precio no encontrado"

    try:
        # A. HACER LA PETICIÓN (REQUEST)
        # ------------------------------
        # 'User-Agent' es una máscara. Le decimos a la web "Soy Chrome en Windows", 
        # para que no nos bloquee por ser un robot de Python.
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10) # timeout=10seg para no colgarse
        
        # VALIDACIÓN DEL STATUS CODE (NUEVO)
        if response.status_code != 200:
            print(f"[FALLO] {proveedor}-{codigo} | Error HTTP {response.status_code} | {url}")
            log_error(proveedor, codigo, url, "Error HTTP", f"Status Code: {response.status_code}")
            continue 
        
        # B. ANALIZAR EL HTML (PARSE)
        # ---------------------------
        soup = BeautifulSoup(response.content, 'html.parser')

        # C. BUSCAR INFORMACIÓN (SELECTORES)
        # ----------------------------------
        # Buscamos el elemento HTML usando el selector CSS que está en tu CSV (columna 'nombre')
        name_element = soup.select_one(row['nombre'])
        if name_element: 
            final_name = name_element.get_text(strip=True)

        # Buscamos el precio
        price_element = soup.select_one(row['monto'])
        if price_element: 
            # Normalizamos también aquí: quitamos \xa0
            # IMPORTANTE: Usamos separator=',' para manejar casos donde los centavos están en tags <sup>
            # Ejemplo CAS: $6.609 <sup>88</sup> -> $6.609,88
            raw_price = price_element.get_text(separator=',', strip=True).replace('\xa0', ' ')
            # Limpieza extra: si el separador quedó pegado al símbolo de moneda ($,100 -> $100)
            final_price = raw_price.replace('$,', '$').replace(' ,', ' ')
        
        # D. LÓGICA DE NEGOCIO (VALIDACIÓN Y CAMBIOS)
        # -------------------------------------------
        # Clave unificada como STRING
        key = (str(proveedor), str(codigo), str(url))
        last_known_price = last_prices.get(key)
        
        # Validaciones: ¿Es un dato útil?
        is_valid_name = final_name != "Nombre no encontrado"
        is_valid_price = final_price != "Precio no encontrado"
        is_non_zero = final_price not in ["$0,00", "$ 0.00"] # Filtramos precios 0

        if is_valid_name and is_valid_price and is_non_zero:
            # ¿Cambió el precio respecto a lo que sabíamos?
            if last_known_price != final_price:
                status = "NUEVO" if last_known_price is None else "CAMBIO"
                print(f"[{status}] {proveedor}-{codigo}: {last_known_price} -> {final_price}")
                
                # Agregamos a la lista de "cosas para guardar"
                new_data.append({
                    'proveedor': proveedor,
                    'codigo': codigo,
                    'codigo_interno': codigo_interno, # Guardamos el código agrupador
                    'nombre_detectado': final_name,
                    'precio_detectado': final_price,
                    'timestamp': timestamp,
                    'url': url 
                })
                # Actualizamos nuestra memoria rápida para no detectarlo de nuevo en esta misma corrida
                last_prices[key] = final_price 
            else:
                # Si el precio es igual, solo avisamos en pantalla, NO guardamos.
                print(f"[IGUAL]  {proveedor}-{codigo}: Mantiene {final_price}")
        else:
             # Si los datos están mal, logueamos el error y saltamos.
             msg = f"[SKIP] {proveedor}-{codigo} | Motivo: Datos incompletos ({final_name}, {final_price})"
             print(msg)
             log_error(proveedor, codigo, url, "Datos incompletos", f"Nombre: {final_name} | Precio: {final_price}")

    except Exception as e:
        # Si explota la conexión o algo técnico falla:
        msg = f"[FALLO] {proveedor}-{codigo} | Error Python: {e}"
        print(msg)
        log_error(proveedor, codigo, url, "Excepción Python", str(e))

# ---------------------------------------------------------
# 5. GUARDADO DE DATOS (PERSISTENCIA)
# ---------------------------------------------------------
if new_data:
    df_new = pd.DataFrame(new_data)
    
    # Truco: Si el archivo NO existe, ponemos cabecera (header=True).
    # Si YA existe, no ponemos cabecera (header=False) y solo agregamos filas (mode='a').
    header_mode = not os.path.exists(HISTORY_FILE)
    
    df_new.to_csv(HISTORY_FILE, mode='a', index=False, header=header_mode)
    print(f"\n¡Listo! Se agregaron {len(new_data)} nuevos registros a '{HISTORY_FILE}'.")
else:
    print("\nNo hubo variaciones de precio. El archivo no se ha tocado.")
