# ---------------------------------------------------------
# UTILS.PY - Funciones compartidas entre scripts
# ---------------------------------------------------------

def clean_price(price_str):
    """
    Convierte un string de precio (eg: "$ 1,234.56" o "$1.234,56") a un float.
    Maneja los distintos formatos que usan los proveedores argentinos.

    Retorna float o None si no se puede parsear o el valor es <= 0.
    """
    import pandas as pd
    if pd.isna(price_str):
        return None
    s = str(price_str).replace('$', '').replace(' ', '').strip()
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
