import pandas as pd
from sqlalchemy import create_engine
import unicodedata
import re
import os

# --- Configuración de archivos y base de datos ---
INPUT_FILE_CIUDADES = 'datos.txt'
DATABASE_NAME_CIUDADES = 'ciudades.db'
NORMALIZED_TABLE_CIUDADES = 'ciudades_norm'

# --- Funciones Auxiliares ---
def remove_accents(text):
    """
    Elimina tildes y acentos de un texto.
    Verifica que el valor sea un texto (tipo str).
    Convierte el texto a una forma especial donde las letras con tilde se separan (por ejemplo: é → e + ́).
    Si no era texto, lo devuelve sin cambios.
    Devuelve el mismo texto pero sin tildes ni acentos.
    """
    if isinstance(text, str):
        text_norm = unicodedata.normalize('NFKD', text)
        return ''.join([c for c in text_norm if not unicodedata.combining(c)])
    return text

# --- 1. Extracción de Datos ---
def extract_data_ciudades(file_path):
    """
    Lee un archivo CSV y lo carga como DataFrame.
    Devuelve los datos como tabla (DataFrame) o None si falló o el archivo está vacío.
    """
    print(f"\n✨ Extrayendo datos de ciudades desde: {file_path}")
    print(f"DEBUG: Directorio de trabajo actual para extracción: {os.getcwd()}")
    print(f"DEBUG: Ruta absoluta del archivo a extraer: {os.path.abspath(file_path)}")
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"⚠️ Advertencia: El archivo '{file_path}' se leyó, pero está vacío o no contiene datos válidos.")
            return None
        print("✅ Datos de ciudades extraídos exitosamente.")
        print(f"DEBUG: {len(df)} filas extraídas inicialmente.")
        return df
    except FileNotFoundError:
        print(f"❌ Error: El archivo de entrada '{file_path}' para ciudades NO FUE ENCONTRADO.")
        print(f"DEBUG: Por favor, asegúrese de que '{file_path}' existe en {os.getcwd()} o su ruta completa es correcta.")
        return None
    except pd.errors.EmptyDataError:
        print(f"❌ Error: El archivo '{file_path}' está vacío o no contiene datos CSV válidos.")
        return None
    except Exception as e:
        print(f"❌ Error general al leer el archivo de ciudades: {e}")
        return None

# --- 2. Transformación de Datos ---
def transform_data_ciudades(df):
    """
    Normaliza texto, elimina tildes y duplicados para los datos de ciudades.
    Recibe un DataFrame (df) con los datos crudos.
    """
    if df is None or df.empty:
        print("⚠️ No hay datos válidos para transformar en el proceso de ciudades. Saltando transformación.")
        return None
    
    print("🔄 Iniciando transformación de datos de ciudades...")
    initial_rows = len(df)

    # Convertir a mayúsculas y asegurar tipo string
    df['nombre_ciudad'] = df['nombre_ciudad'].astype(str).str.upper() 
    df['pais'] = df['pais'].astype(str).str.upper() 
    print("  - Texto de ciudades convertido a mayúsculas.")

    # Remover tildes y acentos
    df['nombre_ciudad'] = df['nombre_ciudad'].apply(remove_accents)
    df['pais'] = df['pais'].apply(remove_accents)
    print("  - Tildes de ciudades eliminadas.")

    # Eliminar espacios extra y caracteres innecesarios
    df['nombre_ciudad'] = df['nombre_ciudad'].str.strip().str.replace(r'\s+', ' ', regex=True)
    df['pais'] = df['pais'].str.strip().str.replace(r'\s+', ' ', regex=True)
    print("  - Espacios y caracteres innecesarios de ciudades limpiados.")

    # Eliminar duplicados
    filas_antes_dup = len(df)
    df.drop_duplicates(subset=['nombre_ciudad', 'pais'], inplace=True)
    deduplicated_rows = len(df)
    print(f"  - Duplicados de ciudades eliminados: {filas_antes_dup - deduplicated_rows} filas removidas.")

    if df.empty:
        print("⚠️ Advertencia: El DataFrame de ciudades quedó vacío después de la transformación (posibles duplicados excesivos).")
        return None

    print(f"✅ Transformación de datos de ciudades completada. {len(df)} filas restantes.")
    return df 

# --- 3. Carga de Datos ---
def load_data_ciudades(df, database_name, table_name):
    """
    Carga los datos transformados de ciudades en una base de datos SQLite.
    Guarda los datos limpios en una base de datos SQLite.
    """
    if df is None or df.empty:
        print("❌ No hay datos válidos de ciudades para cargar. Saltando carga.")
        return

    print(f"📦 Cargando datos de ciudades en '{table_name}' dentro de '{database_name}'...")
    print(f"DEBUG: Ruta de la base de datos de ciudades: {os.path.abspath(database_name)}")
    engine = create_engine(f'sqlite:///{database_name}')
    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"✅ Datos de ciudades cargados exitosamente. {len(df)} filas insertadas.")
    except Exception as e:
        print(f"❌ Error al cargar los datos de ciudades: {e}")

# --- Orquestador ETL --- 
def run_etl_ciudades():
    """
    Ejecuta el proceso ETL completo para datos de ciudades: extracción, transformación y carga.
    Si no hay un archivo datos.txt, crea uno nuevo con ejemplos para pruebas.
    """
    print("\n--- INICIANDO PROCESO ETL DE CIUDADES ---")

    # Crear archivo de ejemplo si no existe (solo para pruebas)
    if not os.path.exists(INPUT_FILE_CIUDADES):
        print(f"ℹ️ El archivo '{INPUT_FILE_CIUDADES}' no fue encontrado. Creando archivo de ejemplo para pruebas...")
        try:
            with open(INPUT_FILE_CIUDADES, 'w', encoding='utf-8') as f:
                f.write("id,nombre_ciudad,pais,poblacion\n")
                f.write("1,BUENOS AIRES,ARGENTINA,15000000\n")
                f.write("2,Sao Paulo,Brasil,22000000\n")
                f.write("3, MÉXICO D.F.,México,21000000\n")
                f.write("4, Santiago, Chile,6000000\n")
                f.write("5, buenos aires,argentina,14999999\n")  # Duplicado
                f.write("6, Bogotá,Colombia,8000000\n")
                f.write("7, LIMA,PERÚ,10000000\n")
                f.write("8,méxico d.f.,méxico,20999999\n")       # Duplicado
                f.write("9,santiago,chile,5999999\n")            # Duplicado
                f.write("10, Ciudad de Panamá.,Panamá,1800000\n") # Punto especial
            print(f"✅ Archivo '{INPUT_FILE_CIUDADES}' creado exitosamente en {os.getcwd()}.")
        except Exception as e:
            print(f"❌ Error al crear el archivo de ejemplo '{INPUT_FILE_CIUDADES}': {e}")
            print("❌ El proceso ETL de ciudades no puede continuar sin el archivo de entrada.")
            return 
    else:
        print(f"ℹ️ Archivo '{INPUT_FILE_CIUDADES}' encontrado. Usando archivo existente.")


    # Paso 1: Extracción
    raw_data = extract_data_ciudades(INPUT_FILE_CIUDADES)
    if raw_data is None:
        print("❌ Extracción de datos de ciudades fallida o archivo vacío. Proceso ETL abortado.")
        print("--- PROCESO ETL DE CIUDADES FINALIZADO CON ERRORES/ADVERTENCIAS ---\n")
        return 

    # Paso 2: Transformación
    transformed_data = transform_data_ciudades(raw_data)
    if transformed_data is None:
        print("❌ Transformación de datos de ciudades resultó en un DataFrame vacío. Proceso ETL abortado.")
        print("--- PROCESO ETL DE CIUDADES FINALIZADO CON ERRORES/ADVERTENCIAS ---\n")
        return 

    # Paso 3: Carga
    load_data_ciudades(transformed_data, DATABASE_NAME_CIUDADES, NORMALIZED_TABLE_CIUDADES)

    print("--- PROCESO ETL DE CIUDADES FINALIZADO ---\n")

    # Verificación final de datos cargados
    try:
        engine = create_engine(f'sqlite:///{DATABASE_NAME_CIUDADES}')
        # Intentar leer la tabla. Si no existe o está vacía, pd.read_sql_table podría lanzar un error o devolver un DF vacío.
        df_check = pd.read_sql_table(NORMALIZED_TABLE_CIUDADES, con=engine)
        if df_check.empty:
            print(f"⚠️ Advertencia: La tabla '{NORMALIZED_TABLE_CIUDADES}' en '{DATABASE_NAME_CIUDADES}' está vacía después de la carga.")
        else:
            print("\n📊 Contenido de la tabla normalizada de ciudades (Verificación):")
            print(df_check)
    except Exception as e:
        print(f"❌ Error al leer la tabla de verificación de ciudades: {e}")
        print(f"DEBUG: Puede que la tabla '{NORMALIZED_TABLE_CIUDADES}' no se haya creado o no contenga datos.")

# --- Código de demostración (se ejecuta solo si este archivo es el principal) ---
if __name__ == "__main__":
    # Para probar este módulo directamente, considera eliminar el archivo 'datos.txt' y 'ciudades.db'
    # para asegurar una ejecución limpia cada vez.
    # if os.path.exists(INPUT_FILE_CIUDADES):
    #     os.remove(INPUT_FILE_CIUDADES)
    # if os.path.exists(DATABASE_NAME_CIUDADES):
    #     os.remove(DATABASE_NAME_CIUDADES)
    run_etl_ciudades()
