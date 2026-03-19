# ---------------------------------------------------------
# UTILS.PY - Funciones compartidas entre scripts
# ---------------------------------------------------------

def clean_price(price_str):
    """
    Convierte un string de precio (eg: "$1.234,56") a un float.
    Maneja los distintos formatos que usan los proveedores argentinos.
    
    Retorna float o None si no se puede parsear.
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
        # Solo coma: puede ser decimal (1234,56) o miles (1,234)
        s = s.replace(',', '.')
    elif '.' in s:
        # Solo punto: puede ser decimal (1234.56) o miles (1.234)
        # Heurística: si hay exactamente 3 dígitos después del punto, es miles
        parts = s.split('.')
        if len(parts) == 2 and len(parts[1]) == 3:
            s = s.replace('.', '')  # es separador de miles
        # else: es separador decimal, no tocar
    try:
        val = float(s)
        # Normalización: algunos sitios mandan el valor en centavos (ej: 650000 = $650)
        if val > 500000:
            val = val / 1000
        return val if val > 50 else None
    except (ValueError, TypeError):
        return None
