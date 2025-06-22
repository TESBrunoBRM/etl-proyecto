import pandas as pd
import sqlite3
import re
import os
import unicodedata # Importar para eliminar acentos en los nombres de columna

# --- Configuración de archivos y base de datos ---
INPUT_FILE_UBICACION = 'DATOS3.txt'
DATABASE_NAME_UBICACION = 'datos_ubicacion.db'
TABLE_LUGARES = 'Lugares'
TABLE_GEOREFERENCIAS = 'Georeferencias'
TABLE_DIRECCIONES = 'Direcciones'

# --- Funciones Auxiliares ---
def remove_accents(text):
    """
    Elimina tildes y acentos de un texto.
    """
    if isinstance(text, str):
        text_norm = unicodedata.normalize('NFKD', text)
        return ''.join([c for c in text_norm if not unicodedata.combining(c)])
    return text

def parse_direccion(direccion):
    """
    Parsea una cadena de dirección completa para extraer nombre de calle,
    número, ciudad/estado/provincia y país.
    Maneja casos donde la dirección puede estar vacía o mal formada.
    """
    if not isinstance(direccion, str) or not direccion.strip():
        return ("", "", "", "") # Devuelve tuplas vacías si la dirección es inválida

    partes = [p.strip() for p in direccion.split(',')]
    
    # Si hay menos de 2 partes significativas, es probable que esté incompleta
    # Considera una dirección válida si tiene al menos una parte no vacía que no sea solo un número
    if len(partes) < 2 and not any(p for p in partes if p and not p.isdigit()):
        return ("", "", "", "") 
    
    # Extraer nombre y número de calle (si es posible)
    match = re.match(r"(.+?)\s*(\d+)?$", partes[0])
    if match:
        nombre_calle = match.group(1).strip()
        numero_calle = match.group(2) if match.group(2) else ""
    else:
        nombre_calle = partes[0]
        numero_calle = ""

    # Ciudad, estado o provincia (todo lo que esté entre medio)
    ciudad_estado = ", ".join(partes[1:-1]) if len(partes) > 2 else (partes[1] if len(partes) > 1 else "")

    # Última parte es el país
    pais = partes[-1] if partes else ""

    return nombre_calle, numero_calle, ciudad_estado, pais

# --- Orquestador ETL --- 
def run_etl_ubicacion():
    """
    Ejecuta el proceso ETL completo para datos de ubicación:
    Extracción, limpieza, normalización y carga a múltiples tablas SQLite.
    """
    print("\n--- INICIANDO PROCESO ETL DE UBICACIÓN ---")

    # --- Gestión de la base de datos (se sigue eliminando para asegurar consistencia de IDs) ---
    if os.path.exists(DATABASE_NAME_UBICACION):
        print(f"DEBUG: Eliminando base de datos existente '{DATABASE_NAME_UBICACION}'.")
        try:
            os.remove(DATABASE_NAME_UBICACION)
        except OSError as e:
            print(f"❌ Error al eliminar '{DATABASE_NAME_UBICACION}': {e}. ¡Asegúrate de que no esté abierto en otra aplicación!")
            print("❌ El proceso ETL de ubicación no puede continuar con una base de datos antigua que podría causar inconsistencias.")
            return
    # -----------------------------------------------------------------------------------------

    # --- Gestión del archivo de entrada (ahora NO se elimina si existe) ---
    if not os.path.exists(INPUT_FILE_UBICACION):
        print(f"ℹ️ El archivo '{INPUT_FILE_UBICACION}' no fue encontrado. Creando archivo de ejemplo para pruebas...")
        try:
            with open(INPUT_FILE_UBICACION, 'w', encoding='latin1') as f: # Usar latin1 como en el original
                # --- Encabezado del archivo de datos ---
                f.write("Nombre del lugar;Dirección Completa;Georeferencia\n") # Usar el encabezado tal como se ve en tu error
                # --- Datos de ejemplo ---
                f.write("Parque Central;Avenida Siempre Viva 123, Springfield, USA;40.7128,-74.0060\n")
                f.write("Museo Nacional;Calle Falsa 123, Ciudad Capital, Provincia X, País Imaginario;34.0522,-118.2437\n")
                f.write("Biblioteca Municipal;Plaza Mayor S/N, Pueblo Chico, Region Sur, País Lejano;51.5074,-0.1278\n")
                f.write("Estadio Grande;Av. Principal 45, Gran Urbe, Estado Grande, País Grande; -23.5505,-46.6333\n")
                f.write("Restaurante El Sabor;Carrera 7 8-9, Bogota, Colombia;4.7110,-74.0721\n")
                f.write("Cafeteria Dulce;Calle de la Amargura 5, Quito, Ecuador;0.1807,-78.4678\n")
                f.write("Panaderia Artesanal;Avenida Siempre Viva 123, Springfield, USA;40.7128,-74.0060\n") # Duplicado
                f.write("Tienda de Souvenirs;Centro Histórico s/n, Lima, Perú;-12.0464,-77.0428\n")
                f.write("Hotel de Lujo;Paseo de la Reforma 1, CDMX, México;19.4326,-99.1332\n")
                f.write("Terminal de Buses;Ruta 5 Norte Km 10, Santiago, Chile;-33.4489,-70.6693\n")
                f.write(";;;\n") # Fila con solo delimitadores (para probar el "fila 9 que no hay nada")
                f.write("Beijing;Ciudad de Beijing, China;39.9042,116.4074\n") # Entrada de Beijing
                f.write("Gran Muralla;Beijing, China;39.9042,116.4074\n") # Otra ubicación en Beijing con mismas coordenadas
                f.write("Beijing;Calle 1, Beijing, China;39.9042,116.4074\n") # Duplicado de Beijing para probar
            print(f"✅ Archivo '{INPUT_FILE_UBICACION}' creado exitosamente en {os.getcwd()}.")
        except Exception as e:
            print(f"❌ Error al crear el archivo de ejemplo '{INPUT_FILE_UBICACION}': {e}")
            print("❌ El proceso ETL de ubicación no puede continuar sin el archivo de entrada.")
            return
    else:
        print(f"ℹ️ Archivo '{INPUT_FILE_UBICACION}' encontrado. Usando archivo existente.")

    # ------------------------------------------------------
    # PASO 1: Extracción de Datos
    # ------------------------------------------------------
    print(f"✨ Extrayendo datos de ubicación desde: {INPUT_FILE_UBICACION}")
    try:
        # Usar skip_blank_lines=True para ignorar líneas completamente vacías al leer
        # Explicitamente indicar que la primera fila es el encabezado
        df = pd.read_csv(INPUT_FILE_UBICACION, sep=";", encoding="latin1", skip_blank_lines=True, header=0)
        
        # Normalizar los nombres de las columnas leídas para hacerlos consistentes
        original_columns = df.columns.tolist()
        normalized_columns = []
        for col in original_columns:
            normalized_col = remove_accents(col) # Eliminar acentos
            normalized_col = normalized_col.strip() # Quitar espacios al inicio/fin
            normalized_col = re.sub(r'\s+', '_', normalized_col) # Reemplazar espacios por guiones bajos
            normalized_col = normalized_col.lower() # Convertir a minúsculas
            normalized_columns.append(normalized_col)
        df.columns = normalized_columns
        
        # Verificar que las columnas esperadas estén presentes después de normalizar el encabezado
        # AQUI ES DONDE SE CORRIGE EL NOMBRE DE LA COLUMNA ESPERADA
        expected_columns = ['nombre_del_lugar', 'direccion_completa', 'georeferencia']
        if not all(col in df.columns for col in expected_columns):
            print(f"❌ Error: El archivo '{INPUT_FILE_UBICACION}' no contiene las columnas esperadas después de la normalización del encabezado.")
            print(f"DEBUG: Columnas encontradas (normalizadas): {df.columns.tolist()}")
            print(f"DEBUG: Columnas esperadas: {expected_columns}")
            return

        # Asegurarse de que el DataFrame no esté vacío después de leer
        if df.empty:
            print(f"⚠️ Advertencia: El archivo '{INPUT_FILE_UBICACION}' se leyó (posiblemente solo el encabezado), pero no contiene datos válidos.")
            return
        
        print("✅ Datos de ubicación extraídos exitosamente.")
        print("\nDEBUG: Primeras filas del DataFrame DESPUÉS DE LECTURA Y NORMALIZACIÓN DE COLUMNAS:")
        print(df.head().to_string(index=False)) 
        print(f"DEBUG: DataFrame tiene {len(df)} filas.")
        print("\n")

    except FileNotFoundError:
        print(f"❌ Error: El archivo de entrada '{INPUT_FILE_UBICACION}' para ubicación no fue encontrado.")
        return
    except pd.errors.EmptyDataError:
        print(f"❌ Error: El archivo '{INPUT_FILE_UBICACION}' está vacío o no contiene datos válidos CSV/TSV.")
        return
    except Exception as e:
        print(f"❌ Error al leer el archivo de ubicación: {e}")
        return

    # ------------------------------------------------------
    # PASO 1.5: Limpieza defensiva de filas que puedan ser encabezados duplicados como datos
    # Esto ocurre si el header se repite en la data o si la lectura inicial falló en identificarlo
    # ------------------------------------------------------
    rows_before_defensive_clean = len(df)
    # Definir los valores de encabezado normalizados que NO deberían aparecer como datos
    # Usar los nombres de columna normalizados para la comparación
    header_values_to_remove = {
        'nombre_del_lugar': remove_accents("Nombre del lugar").strip().replace(' ', '_').lower(),
        'direccion_completa': remove_accents("Dirección Completa").strip().replace(' ', '_').lower(),
        'georeferencia': remove_accents("Georeferencia").strip().replace(' ', '_').lower()
    }
    
    # Crear una máscara booleana para identificar las filas que coinciden exactamente con los valores del encabezado
    # Se asegura que la comparación sea con el tipo str para evitar errores si hay NaN
    mask_is_header_row = df.apply(lambda row: all(str(row[col]).strip().lower() == header_values_to_remove[col] 
                                                  for col in expected_columns), axis=1)
    
    df = df[~mask_is_header_row] # Filtrar para remover esas filas
    
    rows_after_defensive_clean = len(df)
    if rows_before_defensive_clean > rows_after_defensive_clean:
        print(f"  - Filas que duplicaban valores de encabezado eliminadas: {rows_before_defensive_clean - rows_after_defensive_clean} filas.")
    else:
        print("  - No se encontraron filas con valores de encabezado duplicados para eliminar.")

    print("\nDEBUG: Primeras filas del DataFrame DESPUÉS DE LIMPIEZA DEFENSIVA DE ENCABEZADOS:")
    print(df.head().to_string(index=False)) 
    print(f"DEBUG: DataFrame tiene {len(df)} filas.")
    print("\n")

    if df.empty:
        print(f"⚠️ Advertencia: Después de la limpieza defensiva, el DataFrame de ubicación está vacío. Proceso ETL abortado.")
        return # Si el DF está vacío, no hay nada más que hacer.

    print("🔄 Iniciando transformación de datos de ubicación...")

    # ------------------------------------------------------
    # PASO 2: Limpiar filas con datos principales vacíos/nulos
    # ------------------------------------------------------
    initial_rows = len(df)
    
    df['nombre_lugar'] = df['nombre_del_lugar'].astype(str).str.strip().replace('', pd.NA) # Usar la columna correcta
    df['direccion_completa'] = df['direccion_completa'].astype(str).str.strip().replace('', pd.NA)
    df['georeferencia'] = df['georeferencia'].astype(str).str.strip().replace('', pd.NA)

    df.dropna(subset=['nombre_lugar', 'direccion_completa', 'georeferencia'], how='all', inplace=True) # Actualizar subset de dropna
    
    cleaned_rows = len(df)
    if initial_rows > cleaned_rows:
        print(f"  - Filas completamente vacías o con datos principales nulos/vacíos eliminadas: {initial_rows - cleaned_rows} filas removidas.")
    else:
        print("  - No se encontraron filas completamente vacías o con datos principales nulos/vacíos para eliminar en esta etapa.")

    print("\nDEBUG: Primeras filas del DataFrame DESPUÉS DE LIMPIEZA DE VACÍOS/NULOS:")
    print(df.head().to_string(index=False)) 
    print(f"DEBUG: DataFrame tiene {len(df)} filas.")
    print("\n")

    if df.empty:
        print(f"⚠️ Advertencia: Después de la limpieza inicial, el DataFrame de ubicación está vacío. Proceso ETL abortado.")
        return # Si el DF está vacío, no hay nada más que hacer.


    # ------------------------------------------------------
    # PASO 3: Separar georeferencia en latitud y longitud
    # ------------------------------------------------------
    df[['latitud', 'longitud']] = df['georeferencia'].str.split(',', expand=True)
    df['latitud'] = pd.to_numeric(df['latitud'].str.strip(), errors='coerce') # 'coerce' convierte errores a NaN
    df['longitud'] = pd.to_numeric(df['longitud'].str.strip(), errors='coerce')
    print("  - Georeferencias separadas en latitud y longitud.")
    
    # ------------------------------------------------------
    # PASO 4: Normalizar la dirección
    # Extraer nombre_calle, numero_calle, ciudad_estado_provincia, país
    # La columna 'pais' se generará aquí.
    # ------------------------------------------------------
    df[['nombre_calle', 'numero_calle', 'ciudad_estado_provincia', 'pais']] = df.apply(
        lambda row: pd.Series(parse_direccion(row['direccion_completa'])), axis=1
    )
    print("  - Dirección completa parseada y normalizada (incluyendo país).")

    # --- Normalizar columnas clave para deduplicación ---
    # Asegurar que todas las columnas usadas para deduplicación estén limpias
    df['nombre_lugar'] = df['nombre_lugar'].astype(str).str.strip().str.upper()
    df['pais'] = df['pais'].astype(str).str.strip().str.upper()


    # ------------------------------------------------------
    # PASO 5: Eliminar duplicados lógicos (basado en nombre de lugar, país y coordenadas)
    # ------------------------------------------------------
    filas_antes_dup_logical = len(df)
    
    # Rellenar NaN en lat/lon temporalmente para que drop_duplicates los trate como iguales
    # Se usa un valor que es muy poco probable que aparezca en coordenadas reales
    df['latitud'].fillna(-999999.0, inplace=True) 
    df['longitud'].fillna(-999999.0, inplace=True) 

    # Realizar la deduplicación
    df.drop_duplicates(subset=['nombre_lugar', 'pais', 'latitud', 'longitud'], inplace=True)
    
    # Restaurar los NaN después de la deduplicación
    df['latitud'].replace(-999999.0, pd.NA, inplace=True)
    df['longitud'].replace(-999999.0, pd.NA, inplace=True)


    if filas_antes_dup_logical > len(df):
        print(f"  - Duplicados lógicos (nombre, país, geo) eliminados: {filas_antes_dup_logical - len(df)} filas removidas.")
    else:
        print("  - No se encontraron duplicados lógicos significativos para eliminar en esta etapa.")
    
    print("\nDEBUG: Primeras filas del DataFrame DESPUÉS DE DEDUPLICACIÓN LÓGICA:")
    print(df.head().to_string(index=False)) 
    print(f"DEBUG: DataFrame tiene {len(df)} filas.")
    print("\n")

    # Reportar cuántas filas tienen lat/lon inválidas DESPUÉS de la separación y deduplicación
    invalid_geo_rows = df['latitud'].isna().sum() + df['longitud'].isna().sum()
    if invalid_geo_rows > 0:
        print(f"  ⚠️ Advertencia: {invalid_geo_rows} entradas restantes con latitud/longitud inválida. Serán omitidas en la carga a Georeferencias.")


    # ------------------------------------------------------
    # PASO 6: Asignar ID únicos (después de todas las limpiezas y deduplicaciones)
    # ------------------------------------------------------
    df = df.reset_index(drop=True) # Resetear índice para asegurar IDs secuenciales comenzando desde 0
    df['id'] = df.index + 1  # ID autoincremental que ahora sí debe empezar en 1
    # Antes de renombrar, asegúrate de que la columna 'nombre_lugar' que se va a utilizar exista
    # y que 'nombre_del_lugar' ya no sea necesaria o se haya transformado.
    # Si 'nombre_lugar' se creó a partir de 'nombre_del_lugar' en un paso anterior,
    # esta línea es redundante. Si 'nombre_del_lugar' aún existe y es la que contiene los datos,
    # entonces se debe renombrar para que los pasos posteriores usen el nombre esperado.
    if 'nombre_del_lugar' in df.columns and 'nombre_lugar' not in df.columns:
        df.rename(columns={'nombre_del_lugar': 'nombre_lugar'}, inplace=True)


    print("  - IDs únicos asignados a los datos limpios y finales.")
    print("\nDEBUG: Primeras filas del DataFrame DESPUÉS DE ASIGNACIÓN DE IDS:")
    print(df.head().to_string(index=False)) 
    print(f"DEBUG: DataFrame tiene {len(df)} filas.")
    print("\n")
    
    print("✅ Transformación de datos de ubicación completada.\n")


    # ------------------------------------------------------
    # PASO 7: Conectar a SQLite y crear tablas si no existen
    # ------------------------------------------------------
    print(f"📦 Cargando datos de ubicación en la base de datos '{DATABASE_NAME_UBICACION}'...")
    conn = None # Inicializar conn a None
    try:
        conn = sqlite3.connect(DATABASE_NAME_UBICACION)
        cursor = conn.cursor()

        # Tabla Lugares
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_LUGARES} (
            id INTEGER PRIMARY KEY,
            nombre_lugar TEXT
        )
        """)

        # Tabla Georeferencias
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_GEOREFERENCIAS} (
            id INTEGER PRIMARY KEY,
            latitud REAL,
            longitud REAL
        )
        """)

        # Tabla Direcciones
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DIRECCIONES} (
            id INTEGER PRIMARY KEY,
            nombre_calle TEXT,
            numero_calle TEXT,
            ciudad_estado_provincia TEXT,
            pais TEXT
        )
        """)
        print("  - Tablas SQLite creadas/verificadas.")

        # ------------------------------------------------------
        # PASO 8: Insertar los datos procesados en las 3 tablas
        # ------------------------------------------------------
        inserted_rows_count = 0
        for _, row in df.iterrows():
            # Saltar si hay latitud o longitud inválida (NaN), ya que Georeferencias lo requiere
            if pd.isna(row['latitud']) or pd.isna(row['longitud']):
                # Ya se reportó en transformación, aquí solo se omite la inserción en Georeferencias
                # Pero intentamos insertar en Lugares y Direcciones si tienen datos
                if pd.isna(row['nombre_lugar']) and pd.isna(row['nombre_calle']):
                    # Si no hay ni lugar ni calle, esta fila es completamente inútil
                    print(f"DEBUG: Omitiendo inserción de fila con ID {row['id']} debido a falta de datos útiles (ni geo, ni nombre, ni dirección).")
                    continue
                else:
                    print(f"DEBUG: Fila con ID {row['id']} tiene geo-datos inválidos, insertando solo en Lugares y Direcciones si es posible.")

            # Insertar en tabla Lugares (si el nombre_lugar no es nulo)
            if pd.notna(row['nombre_lugar']):
                cursor.execute(f"INSERT INTO {TABLE_LUGARES} (id, nombre_lugar) VALUES (?, ?)",
                               (row['id'], row['nombre_lugar']))

            # Insertar en tabla Georeferencias (solo si latitud y longitud son válidas)
            if pd.notna(row['latitud']) and pd.notna(row['longitud']):
                cursor.execute(f"INSERT INTO {TABLE_GEOREFERENCIAS} (id, latitud, longitud) VALUES (?, ?, ?)",
                               (row['id'], row['latitud'], row['longitud']))

            # Insertar en tabla Direcciones (si al menos el nombre de calle o el país no son nulos)
            if pd.notna(row['nombre_calle']) or pd.notna(row['pais']):
                cursor.execute(f"""
                    INSERT INTO {TABLE_DIRECCIONES} (id, nombre_calle, numero_calle, ciudad_estado_provincia, pais)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row['id'], row['nombre_calle'], row['numero_calle'],
                    row['ciudad_estado_provincia'], row['pais']
                ))
            inserted_rows_count += 1 # Contar las filas que al menos intentaron insertarse

        
        conn.commit()
        print(f"✅ Datos de ubicación cargados exitosamente en las tablas SQLite. ({inserted_rows_count} filas procesadas para inserción)")

        # Verificación final de datos cargados (opcional, para visualización)
        print(f"\n📊 Contenido de la tabla '{TABLE_LUGARES}':")
        print(pd.read_sql_query(f"SELECT * FROM {TABLE_LUGARES}", conn))
        print(f"\n📊 Contenido de la tabla '{TABLE_GEOREFERENCIAS}':")
        print(pd.read_sql_query(f"SELECT * FROM {TABLE_GEOREFERENCIAS}", conn))
        print(f"\n📊 Contenido de la tabla '{TABLE_DIRECCIONES}':")
        print(pd.read_sql_query(f"SELECT * FROM {TABLE_DIRECCIONES}", conn))

    except sqlite3.Error as e:
        print(f"❌ Error de base de datos al cargar datos de ubicación: {e}")
    except Exception as e:
        print(f"❌ Error general al cargar datos de ubicación: {e}")
    finally:
        if conn:
            conn.close()

    print("--- PROCESO ETL DE UBICACIÓN FINALIZADO ---\n")

# --- Código de demostración (se ejecuta solo si este archivo es el principal) ---
if __name__ == "__main__":
    run_etl_ubicacion()
