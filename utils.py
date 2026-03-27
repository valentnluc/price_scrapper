# ---------------------------------------------------------
# UTILS.PY - Funciones compartidas entre scripts
# ---------------------------------------------------------
import re

def clean_price(price_str):
    """
    Convierte un string de precio a float.
    Maneja formatos argentinos ("$1.234,56"), anglosajones ("$ 1,234.56"),
    y precios con unidad al final ("$ 865 x unidad.", "$ 1.200 c/u").

    Retorna float o None si no se puede parsear o el valor es <= 0.
    """
    import pandas as pd
    if pd.isna(price_str):
        return None
    s = str(price_str).replace('$', '').strip()
    if not s:
        return None

    # Eliminar texto de unidad al final (ej: "x unidad.", "c/u", "x m2.", "por unidad")
    # Preserva todo lo anterior al primer espacio + carácter alfabético
    s = re.split(r'\s+[a-zA-ZáéíóúüñÀ-ÿ]', s)[0].strip()

    # Eliminar espacios internos restantes
    s = s.replace(' ', '')
    if not s:
        return None

    # Detectar cuál es el separador decimal (coma o punto)
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            # Formato europeo/argentino: 1.234,56
            s = s.replace('.', '').replace(',', '.')
        else:
            # Formato anglosajón: 1,234.56
            s = s.replace(',', '')
    elif ',' in s:
        # Solo coma: asumimos separador decimal (ej: 1234,56)
        s = s.replace(',', '.')
    elif '.' in s:
        # Solo punto: heurística — si hay exactamente 3 dígitos después del punto, es miles
        parts = s.split('.')
        if len(parts) == 2 and len(parts[1]) == 3:
            s = s.replace('.', '')  # separador de miles → quitar
        # else: separador decimal, no tocar

    try:
        val = float(s)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None
