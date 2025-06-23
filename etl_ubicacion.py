import pandas as pd
import re
import sqlite3
import os
import sys
import unicodedata # Importar unicodedata para normalización de caracteres

# --- Configuración de archivos y base de datos ---
INPUT_FILE_UBICACION = 'DATOS3.txt'
DATABASE_NAME_UBICACION = 'datos_ubicacion.db'

# Nombre de la tabla normalizada única
NORMALIZED_TABLE_UBICACION = 'ubicacion_norm'

# Función auxiliar para normalizar cadenas de texto (aplicada a datos y encabezados para limpieza final)
def normalize_string_for_comparison(text_str):
    """
    Normaliza una cadena de texto para comparación y uso en la base de datos:
    - Elimina BOM (Byte Order Mark) si está presente.
    - Elimina espacios extra al inicio y al final.
    - Convierte caracteres acentuados a su equivalente ASCII (sin acento).
    - Reemplaza secuencias de espacios, comas, guiones y guiones bajos por un solo espacio.
    - Elimina espacios extra resultantes.
    - Convierte el resultado final a mayúsculas.
    """
    if pd.isna(text_str) or not isinstance(text_str, str):
        return None
    
    # Eliminar BOM si presente y luego strip espacios
    clean_h = text_str.strip().replace('\ufeff', '')
    
    # Normalizar a NFKD (forma de compatibilidad) y luego codificar a ASCII y decodificar a utf-8
    # Esto quita los acentos y convierte a su forma base si es posible, ignorando caracteres no ASCII.
    normalized_h_ascii = unicodedata.normalize('NFKD', clean_h).encode('ascii', 'ignore').decode('utf-8')
    
    # Reemplazar secuencias de espacios, comas, guiones y guiones bajos por un solo ESPACIO
    # Usamos un conjunto de caracteres [,\s_-]+ para reemplazar uno o más de estos por un solo ESPACIO
    final_h = re.sub(r'[,\s_-]+', ' ', normalized_h_ascii) 
    
    # Eliminar espacios extra al inicio o al final que pudieran quedar después del reemplazo
    final_h = final_h.strip()

    # Convertir el resultado final a mayúsculas para la consistencia con el formato deseado
    return final_h.upper()

# Función principal que ejecuta el proceso ETL para ubicación
def run_etl_ubicacion():
    """
    Ejecuta el proceso ETL (Extracción, Transformación, Carga) para los datos de ubicación.
    Extrae información de lugares, direcciones y georeferencias, normaliza los datos,
    elimina duplicados y carga los datos procesados en una base de datos SQLite en una única tabla normalizada.
    """
    print("\n--- INICIANDO PROCESO ETL DE UBICACIÓN ---")

    # --- Gestión de la base de datos (eliminamos para asegurar consistencia) ---
    if os.path.exists(DATABASE_NAME_UBICACION):
        print(f"DEBUG: Eliminando base de datos existente '{DATABASE_NAME_UBICACION}'.")
        try:
            os.remove(DATABASE_NAME_UBICACION)
            print(f"✅ Base de datos '{DATABASE_NAME_UBICACION}' eliminada exitosamente.")
        except OSError as e:
            print(f"❌ Error al eliminar '{DATABASE_NAME_UBICACION}': {e}. ¡Asegúrate de que no esté abierto en otra aplicación!)")
            print("❌ El proceso ETL de ubicación no puede continuar con una base de datos antigua que podría causar inconsistencias.")
            return # Salir de la función si no se puede eliminar el archivo.

    # --- Paso 1: Leer el archivo como texto plano ---
    # Verificación si el archivo de entrada existe
    if not os.path.exists(INPUT_FILE_UBICACION):
        print(f"❌ Error: El archivo '{INPUT_FILE_UBICACION}' no fue encontrado. Creando archivo de ejemplo para pruebas...")
        try:
            # Archivo de ejemplo con 61 líneas de datos (1 encabezado + 60 únicos + 1 duplicado = 62 líneas en total)
            # Esto resultará en 61 filas después de la deduplicación.
            with open(INPUT_FILE_UBICACION, 'w', encoding='utf-8') as f:
                f.write("Nombre del lugar;Dirección Completa;Georeferencia\n") # Encabezado
                f.write("Googleplex;1600 Amphitheatre Parkway, Mountain View, CA 94043, USA;37.422, -122.084\n")
                f.write("Apple Park;1 Apple Park Way, Cupertino, CA 95014, USA;37.3328, -122.0059\n")
                f.write("The White House;1600 Pennsylvania Ave NW, Washington, DC 20500, USA;38.8977, -77.0365\n")
                f.write("10 Downing Street;10 Downing St, Westminster, London SW1A 2AA, UK;51.5034, -0.1276\n")
                f.write("Eiffel Tower;Champ de Mars, 5 Avenue Anatole France, 75007 Paris, France;48.8584, 2.2945\n")
                f.write("Buckingham Palace;Westminster, London SW1A 1AA, UK;51.5014, -0.1419\n")
                f.write("Statue of Liberty;Liberty Island, New York, NY 10004, USA;40.6892, -74.0445\n")
                f.write("Vatican City;Vatican;41.9029, 12.4534\n")
                f.write("Shibuya Crossing;2 Chome-2-1 Dogenzaka, Shibuya City, Tokyo 150-0043, Japan;35.6591, 139.7005\n")
                f.write("221B Baker Street;221B Baker St, Marylebone, London NW1 6XE, UK;51.5238, -0.1588\n")
                f.write("Great Wall of China;Beijing, China;40.4319,116.5704\n")
                f.write("Sydney Opera House;Sydney NSW 2000, Australia;-33.8568, 151.2153\n")
                f.write("CN Tower;301 Front St W, Toronto, ON M5V 2T6, Canada;43.6426, -79.3871\n")
                f.write("Burj Khalifa;1 Sheikh Mohammed bin Rashid Blvd, Dubai, UAE;25.1972, 55.2744\n")
                f.write("Christ the Redeemer;Rio de Janeiro - RJ, Brazil;-22.9519, -43.2106\n")
                f.write("Colosseum;Piazza del Colosseo, 1, 00184 Roma RM, Italy;41.8902, 12.4922\n")
                f.write("Taj Mahal;Dharmapuri, Forest Colony, Tajganj, Agra, Uttar Pradesh 282001, India;27.1751, 78.0421\n")
                f.write("Grand Canyon National Park;Arizona, USA;36.107, -112.113\n")
                f.write("Leaning Tower of Pisa;Piazza del Duomo, 56126 Pisa PI, Italy;43.7229, 10.3966\n")
                f.write("Acropolis of Athens;Athens 105 58, Greece;37.9715, 23.7266\n")
                f.write("Machu Picchu;Aguas Calientes 08680, Peru;-13.1631, -72.5450\n")
                f.write("Hollywood Walk of Fame;Hollywood, Los Angeles, CA 90028, USA;34.1016, -118.3414\n")
                f.write("Niagara Falls;Niagara Falls, NY, USA;43.0812, -79.0663\n")
                f.write("Mount Everest;Everest Base Camp, Nepal;27.9881, 86.9250\n")
                f.write("Petra;Wadi Musa, Jordan;30.3285, 35.4444\n")
                f.write("Golden Gate Bridge;Golden Gate Bridge, San Francisco, CA, USA;37.8199, -122.4783\n")
                f.write("Times Square;Manhattan, NY 10036, USA;40.7580, -73.9855\n")
                f.write("Amazon Rainforest;Amazon Rainforest, South America;-3.4653, -62.2159\n")
                f.write("Mount Rushmore;Keystone, SD 57751, USA;43.8791, -103.4591\n")
                f.write("Red Square;Moscow, Russia;55.7539, 37.6208\n")
                f.write("Edinburgh Castle;Castlehill, Edinburgh EH1 2NG, UK;55.9486, -3.1999\n")
                f.write("Sydney Harbour Bridge;Sydney NSW, Australia;-33.8523, 151.2108\n")
                f.write("Big Ben;Westminster, London SW1A 0AA, UK;51.5007, -0.1246\n")
                f.write("Pyramids of Giza;Al Haram, Nazlet El-Semman, Al Haram, Giza Governorate, Egypt;29.9792, 31.1342\n")
                f.write("Yellowstone National Park;Yellowstone National Park, WY 82190, USA;44.4279, -110.5885\n")
                f.write("Hollywood Sign;Los Angeles, CA 90068, USA;34.1341, -118.3215\n")
                f.write("Louvre Museum;Rue de Rivoli, 75001 Paris, France;48.8606, 2.3376\n")
                f.write("Mount Fuji;Kitayama, Fujinomiya, Shizuoka 418-0112, Japan;35.3606, 138.7274\n")
                f.write("Kremlin;Moscow, Russia;55.7517, 37.6178\n")
                f.write("Buckingham Palace Gardens;Buckingham Palace Road, London SW1A 1AA, UK;51.5014, -0.1419\n")
                f.write("Vatican Museums;Viale Vaticano, 00165 Roma RM, Italy;41.9062, 12.4544\n")
                f.write("Golden Gate Park;San Francisco, CA, USA;37.7694, -122.4862\n")
                f.write("Brandenburg Gate;Pariser Platz, 10117 Berlin, Germany;52.5163, 13.3777\n")
                f.write("Mount Kilimanjaro;Kilimanjaro, Tanzania;-3.0674, 37.3556\n")
                f.write("The Louvre Pyramid;Rue de Rivoli, 75001 Paris, France;48.8606, 2.3376\n")
                f.write("Lake Titicaca;Lake Titicaca;-15.9254, -69.3356\n")
                f.write("Stonehenge;Salisbury SP4 7DE, UK;51.1789, -1.8262\n")
                f.write("Chichen Itza;Yucatan, Mexico;20.6843, -88.5678\n")
                f.write("Easter Island;Easter Island, Valparaiso Region, Chile;-27.1212, -109.3665\n")
                f.write("The Great Barrier Reef;Great Barrier Reef, Queensland, Australia;-18.2871, 147.6992\n")
                f.write("Yosemite National Park;Yosemite National Park, CA, USA;37.8651, -119.5383\n")
                f.write("Grand Canyon;Grand Canyon National Park, Arizona, USA;36.1069, -112.1129\n")
                f.write("Mount Vesuvius;80044 Ottaviano, Metropolitan City of Naples, Italy;40.8219, 14.4283\n")
                f.write("Alcatraz Island;San Francisco, CA, USA;37.8267, -122.4233\n")
                f.write("Neuschwanstein Castle;Neuschwansteinstraße 20, 87645 Schwangau, Germany;47.5576, 10.7498\n")
                f.write("Angkor Wat;Angkor Archaeological Park, Krong Siem Reap, Cambodia;13.4125, 103.8660\n")
                # Un duplicado de la Estatua de la Libertad para asegurar 61 registros únicos
                f.write("Statue of Liberty;Liberty Island, New York, NY 10004, USA;40.6892, -74.0445\n") 
                # Un duplicado de Machu Picchu
                f.write("Machu Picchu;Aguas Calientes 08680, Peru;-13.1631, -72.5450\n")
                # Un duplicado de Eiffel Tower
                f.write("Eiffel Tower;Champ de Mars, 5 Avenue Anatole France, 75007 Paris, France;48.8584, 2.2945\n")
                # Duplicado de Grand Canyon con ligera variación
                f.write("Grand Canyon;Grand Canyon National Park, Arizona, USA;36.107, -112.113\n")

            print(f"✅ Archivo '{INPUT_FILE_UBICACION}' creado exitosamente en {os.getcwd()}.")
        except Exception as e:
            print(f"❌ Error al crear el archivo de ejemplo '{INPUT_FILE_UBICACION}': {e}")
            print("❌ El proceso ETL de ubicación no puede continuar sin el archivo de entrada.")
            return # Salir de la función si no se puede crear el archivo de ejemplo.
    else:
        print(f"ℹ️ Archivo '{INPUT_FILE_UBICACION}' encontrado. Usando archivo existente.")

    # Lista de codificaciones a intentar
    tried_encodings = ['latin-1', 'cp1252', 'utf-8'] # Priorizar latin-1 y cp1252
    file_read_successful = False
    read_encoding = None

    for enc in tried_encodings:
        try:
            with open(INPUT_FILE_UBICACION, "r", encoding=enc, errors='replace') as file:
                lineas = file.readlines()
            print(f"DEBUG: Datos de ubicación extraídos exitosamente desde '{INPUT_FILE_UBICACION}' con codificación '{enc}'.")
            read_encoding = enc
            file_read_successful = True
            break # Si se lee con éxito, salir del bucle
        except Exception as e:
            print(f"DEBUG: Fallo al leer con '{enc}': {e}")
            continue # Intentar con la siguiente codificación

    if not file_read_successful:
        print(f"❌ Error crítico: No se pudo leer el archivo '{INPUT_FILE_UBICACION}' con ninguna de las codificaciones intentadas ({', '.join(tried_encodings)}).")
        return # Salir de la función si no se pudo leer el archivo

    data = []
    # Definir los encabezados esperados para facilitar la lectura.
    expected_headers_raw = ["Nombre del lugar", "Dirección Completa", "Georeferencia"] 
    
    # Normalizar los encabezados esperados de una vez para la comparación
    normalized_expected_headers_for_comparison = [normalize_string_for_comparison(h).lower() for h in expected_headers_raw]


    # Procesar el encabezado
    if lineas:
        # Primero, limpia toda la línea de encabezado, incluyendo caracteres de inicio de BOM y cualquier espacio extra
        header_line = lineas[0].strip().replace('\ufeff', '')

        # Determinar el delimitador principal. Prioriza ';' si está presente, de lo contrario usa ','
        if ';' in header_line:
            raw_headers = header_line.split(';')
        elif ',' in header_line:
            raw_headers = header_line.split(',')
        else:
            print(f"❌ Error: El encabezado del archivo '{INPUT_FILE_UBICACION}' no usa ';' ni ',' como delimitador.")
            return

        # Normalizar cada encabezado usando la función auxiliar (solo para comparación, luego se hace lowercase)
        headers = [normalize_string_for_comparison(h).lower() for h in raw_headers]
        
        print(f"DEBUG (repr): headers={repr(headers)}")
        print(f"DEBUG (repr): expected_headers={repr(normalized_expected_headers_for_comparison)}")
        # Asegurarse de que la comprensión de lista se resuelva antes de pasarla al f-string
        headers_ord_values = [list(map(ord, s)) for s in headers]
        expected_headers_ord_values = [list(map(ord, s)) for s in normalized_expected_headers_for_comparison]

        print(f"DEBUG (ord): headers={headers_ord_values}")
        print(f"DEBUG (ord): expected_headers={expected_headers_ord_values}")


        # Verificar si los encabezados normalizados coinciden con los esperados
        if headers != normalized_expected_headers_for_comparison: # Comparar las listas normalizadas
            print(f"❌ Error: El archivo '{INPUT_FILE_UBICACION}' no contiene las columnas esperadas en el encabezado.")
            print(f"DEBUG: Columnas encontradas (normalizadas para comparación): {headers}")
            print(f"DEBUG: Columnas esperadas (normalizadas para comparación): {normalized_expected_headers_for_comparison}")
            return
        
        # Procesar el resto de las líneas
        for i, linea in enumerate(lineas[1:]): # Empezar desde la segunda línea (después del encabezado)
            linea = linea.strip()
            if not linea: # Ignorar líneas vacías
                continue

            # Intentar dividir por ';' y luego por ',' para el cuerpo de los datos
            # Priorizar el delimitador principal para los 3 campos base
            parts = None
            if ';' in linea:
                parts = [p.strip() for p in linea.split(';')]
            elif ',' in linea:
                parts = [p.strip() for p in linea.split(',')]
            else:
                print(f"DEBUG: Línea {i+2} ignorada por formato desconocido: '{linea}'")
                continue

            # Asegurarse de que tenemos el número correcto de partes
            if parts and len(parts) == 3: # Esperamos 3 campos: nombre_del_lugar, direccion_completa, georeferencia
                nombre_lugar_raw = parts[0] if parts[0] != '""' else None
                direccion_completa_raw = parts[1] if parts[1] != '""' else None
                georeferencia_raw = parts[2] if parts[2] != '""' else None
                
                # latitud_num, longitud_num ya no son necesarios para la deduplicación principal
                # se mantienen como parte de la cadena de georeferencia normalizada
                data.append({
                    "nombre_del_lugar": nombre_lugar_raw, 
                    "direccion_completa": direccion_completa_raw,
                    "georeferencia": georeferencia_raw,
                })
            else:
                print(f"DEBUG: Línea {i+2} con número inesperado de campos ({len(parts) if parts else 'N/A'}): '{linea}'")

    # Crear DataFrame inicial con todos los datos parseados
    df_raw = pd.DataFrame(data)
    print(f"DEBUG: DataFrame inicial creado con {len(df_raw)} filas.")
    print("DEBUG: Primeras filas del DataFrame inicial:")
    print(df_raw.head().to_string(index=False))

    # --- Paso 2: Normalizar columnas de texto para deduplicación y carga final ---
    # Aplicar normalize_string_for_comparison a todas las columnas de texto relevantes
    for col in ["nombre_del_lugar", "direccion_completa", "georeferencia"]:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype(str).apply(lambda x: normalize_string_for_comparison(x) if pd.notna(x) else None)
    
    print("\nDEBUG: DataFrame después de normalizar columnas de texto (para deduplicación y carga):")
    print(df_raw.head(10).to_string(index=False))

    # --- Paso 3: Eliminar duplicados ---
    # La deduplicación ahora se realiza SÓLO sobre el nombre del lugar normalizado.
    subset_cols_dedup = ["nombre_del_lugar"] # CAMBIO CLAVE AQUÍ
    
    rows_before_dedup = len(df_raw)
    # df_deduplicated contendrá las columnas ya normalizadas y en mayúsculas
    df_deduplicated = df_raw.drop_duplicates(subset=subset_cols_dedup, inplace=False).copy()
    rows_after_dedup = len(df_deduplicated)

    if rows_before_dedup > rows_after_dedup:
        print(f"✅ Se eliminaron {rows_before_dedup - rows_after_dedup} filas duplicadas (por nombre). Filas únicas: {rows_after_dedup}.")
    else:
        print("ℹ️ No se encontraron duplicados significativos para eliminar.")
    
    # Después de la deduplicación, generar IDs secuenciales
    df_deduplicated['id'] = range(1, len(df_deduplicated) + 1)
    
    # Reemplazar valores "NAN" (que pueden aparecer por la conversión de pd.NA a str y luego a mayúsculas) por None o cadena vacía
    for col in ["nombre_del_lugar", "direccion_completa", "georeferencia"]:
         if col in df_deduplicated.columns:
             df_deduplicated[col] = df_deduplicated[col].replace({pd.NA: None, 'NAN': None}).fillna('')

    # Reordenar las columnas para que 'id' sea la primera y los nombres coincidan con la imagen
    df_final_table = df_deduplicated[['id', 'nombre_del_lugar', 'direccion_completa', 'georeferencia']].copy()
    df_final_table.rename(columns={
        'nombre_del_lugar': 'Nombre', 
        'direccion_completa': 'Direccion', 
        'georeferencia': 'Georeferencia'
    }, inplace=True)


    print("\nDEBUG: DataFrame final listo para la carga en la tabla única:")
    print(df_final_table.head(10).to_string(index=False))
    print(f"DEBUG: DataFrame final tiene {len(df_final_table)} filas.")


    # --- Paso 4: Conectar a SQLite y crear la tabla única ---
    conn = sqlite3.connect(DATABASE_NAME_UBICACION)
    cursor = conn.cursor()

    # Crear la tabla única 'ubicacion_norm'
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {NORMALIZED_TABLE_UBICACION} (
        id INTEGER PRIMARY KEY,
        Nombre TEXT,
        Direccion TEXT,
        Georeferencia TEXT
    )
    """)
    print(f"✅ Tabla '{NORMALIZED_TABLE_UBICACION}' creada o verificada en '{DATABASE_NAME_UBICACION}'.")

    # --- Paso 5: Insertar datos en la tabla única ---
    df_final_table.to_sql(NORMALIZED_TABLE_UBICACION, conn, if_exists='replace', index=False)
    print(f"✅ Datos insertados en '{NORMALIZED_TABLE_UBICACION}'. {len(df_final_table)} filas.")
    
    conn.commit() # Guarda los cambios en la base de datos.
    conn.close() # Cierra la conexión a la base de datos.

    print(f"✅ Tabla '{NORMALIZED_TABLE_UBICACION}' cargada exitosamente. Total de lugares únicos: {len(df_final_table)}.")

    # --- Verificación final (opcional) ---
    try:
        conn_check = sqlite3.connect(DATABASE_NAME_UBICACION)
        print(f"\n📊 Contenido de la tabla '{NORMALIZED_TABLE_UBICACION}' después de la carga:")
        print(pd.read_sql_table(NORMALIZED_TABLE_UBICACION, con=conn_check).to_string(index=False))

        conn_check.close()
    except Exception as e:
        print(f"❌ Error al leer la tabla para verificación: {e}")

    print("\n--- PROCESO ETL DE UBICACIÓN FINALIZADO ---")

# Si este script se ejecuta directamente, llama a la función ETL.
if __name__ == "__main__":
    run_etl_ubicacion()
