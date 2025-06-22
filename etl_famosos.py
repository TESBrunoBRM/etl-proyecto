import pandas as pd
from datetime import datetime
import re
import sqlite3
import os

# --- Configuración de archivos y base de datos ---
INPUT_FILE_FAMOSOS = 'DATOS2.txt'
DATABASE_NAME_FAMOSOS = 'datos_famosos.db'
NORMALIZED_TABLE_FAMOSOS = 'fnac_famosos_norm'

# --- Funciones Auxiliares ---
def transformar_fecha(fecha_raw):
    """
    Normaliza el formato de una fecha cruda a 'DD-MM-YYYY'.
    Ignora fechas que contengan 'alrededor' o 'a.c'.
    """
    fecha_raw = fecha_raw.lower()
    if "alrededor" in fecha_raw or "a.c" in fecha_raw:
        return None
    fecha_raw = fecha_raw.replace(".", "-").replace("/", "-")
    # Intentar varios formatos de fecha
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d-%m-%y", "%Y%m%d"): # Añadido %d-%m-%y para años de 2 dígitos
        try:
            fecha = datetime.strptime(fecha_raw, fmt)
            return fecha.strftime("%d-%m-%Y")
        except ValueError:
            continue
    return None

def calcular_edad(fecha_str):
    """
    Calcula la edad de una persona a partir de su fecha de nacimiento.
    """
    if fecha_str:
        try:
            fecha = datetime.strptime(fecha_str, "%d-%m-%Y")
            hoy = datetime.now()
            # Calcula la edad: año actual - año de nacimiento,
            # luego ajusta si el cumpleaños no ha pasado aún este año.
            return hoy.year - fecha.year - ((hoy.month, hoy.day) < (fecha.month, fecha.day))
        except ValueError:
            return None
    return None

def cumple_hoy(fecha_str):
    """
    Verifica si el cumpleaños es hoy. Devuelve 1 si sí, 0 si no o si la fecha es inválida.
    """
    if fecha_str:
        try:
            fecha = datetime.strptime(fecha_str, "%d-%m-%Y")
            hoy = datetime.now()
            return int((fecha.day == hoy.day) and (fecha.month == hoy.month))
        except ValueError:
            return 0
    return 0

# --- Orquestador ETL --- 
def run_etl_famosos():
    """
    Ejecuta el proceso ETL completo para datos de famosos.
    Incluye extracción, transformación y carga a SQLite.
    """
    print("\n--- INICIANDO PROCESO ETL DE FAMOSOS ---")

    # Crear archivo de ejemplo si no existe (solo para pruebas)
    if not os.path.exists(INPUT_FILE_FAMOSOS):
        print(f"ℹ️ Creando archivo de ejemplo '{INPUT_FILE_FAMOSOS}' para pruebas...")
        with open(INPUT_FILE_FAMOSOS, 'w', encoding='utf-8') as f:
            f.write("1. Leonardo DiCaprio - 11-11-1974\n")
            f.write("2. Scarlett Johansson - 22-11-1984\n")
            f.write("3. Tom Hanks - 09-07-1956\n")
            f.write("4. Meryl Streep - 22-06-1949\n")
            f.write("5. Johnny Depp - 09-06-1963\n")
            f.write("6. Tom Hanks - 09-07-1956\n") # Duplicado
            f.write("7. Angelina Jolie - 04-06-1975\n")
            f.write("8. Brad Pitt - 18-12-1963\n")
            f.write("9. Julia Roberts - 28-10-1967\n")
            f.write("10. Robert Downey Jr. - 04-04-1965\n")
            f.write("11. Gal Gadot - 30-04-1985\n")
            f.write("12. George Clooney - 06-05-1961\n")
            f.write("13. Beyoncé - 04-09-1981\n")
            f.write("14. Will Smith - 25-09-1968\n")
            f.write("15. Chris Evans - 13-06-1981\n")
            f.write("16. Emma Stone - 06-11-1988\n")
            f.write("17. Ryan Gosling - 12-11-1980\n")
            f.write("18. Jennifer Lawrence - 15-08-1990\n")
            f.write("19. Denzel Washington - 28-12-1954\n")
            f.write("20. Natalie Portman - 09-06-1981\n")
            f.write("21. Christian Bale - 30-01-1974\n")
            f.write("22. Anne Hathaway - 12-11-1982\n")
            f.write("23. Joaquin Phoenix - 28-10-1974\n")
            f.write("24. Cate Blanchett - 14-05-1969\n")
            f.write("25. Matt Damon - 08-10-1970\n")
            f.write("26. Sandra Bullock - 26-07-1964\n")
            f.write("27. Keanu Reeves - 02-09-1964\n")
            f.write("28. Nicole Kidman - 20-06-1967\n")
            f.write("29. Russell Crowe - 07-04-1964\n")
            f.write("30. Salma Hayek - 02-09-1966\n")
            f.write("31. Brad Pitt - 18/12/1963\n") # Duplicado con formato diferente
            f.write("32. Un famoso antiguo - 12.03.1890\n") # Fecha antigua
            f.write("33. Famoso sin fecha\n") # Sin fecha
            f.write("34. Otro famoso - alrededor de 1950\n") # "alrededor"
        print(f"✅ Archivo '{INPUT_FILE_FAMOSOS}' creado.")


    # Paso 1: Leer el archivo como texto plano
    print(f"✨ Extrayendo datos de famosos desde: {INPUT_FILE_FAMOSOS}")
    try:
        with open(INPUT_FILE_FAMOSOS, "r", encoding="utf-8") as file:
            lineas = file.readlines()
        print("✅ Datos de famosos extraídos exitosamente.")
    except FileNotFoundError:
        print(f"❌ Error: El archivo de entrada '{INPUT_FILE_FAMOSOS}' para famosos no fue encontrado.")
        return
    except Exception as e:
        print(f"❌ Error al leer el archivo de famosos: {e}")
        return

    # Paso 2: Extraer nombre y fecha de cada línea
    print("🔄 Iniciando transformación de datos de famosos...")
    data = []
    for linea in lineas:
        linea = re.sub(r'^\d+\.\s*', '', linea.strip()) # Eliminar el número y punto al inicio
        if '-' in linea:
            partes = linea.split('-', 1)
            nombre = partes[0].strip()
            fecha = partes[1].strip()
            data.append((nombre, fecha))
        else: # Manejar líneas sin guion (sin fecha)
            nombre = linea.strip()
            data.append((nombre, None)) # Añadir None para la fecha

    # Paso 3: Crear DataFrame
    df = pd.DataFrame(data, columns=["nombre", "fecha_nacimiento_raw"])

    # Paso 4: Normalizar fecha
    df['fecha_nacimiento'] = df['fecha_nacimiento_raw'].apply(transformar_fecha)
    print("  - Fechas normalizadas.")

    # Paso 5: Calcular edad
    df['edad'] = df['fecha_nacimiento'].apply(calcular_edad)
    print("  - Edades calculadas.")

    # Paso 6: Flag cumpleaños
    df['cumple_hoy'] = df['fecha_nacimiento'].apply(cumple_hoy)
    print("  - Flag de cumpleaños añadido.")

    # Paso 7: Eliminar duplicados por nombre y fecha de nacimiento normalizada
    filas_antes = len(df)
    df = df.drop_duplicates(subset=['nombre', 'fecha_nacimiento'])
    print(f"  - Duplicados de famosos eliminados: {filas_antes - len(df)} filas removidas.")
    
    print("✅ Transformación de datos de famosos completada.\n")

    # Paso 8: Conectar a SQLite y crear tabla
    print(f"📦 Cargando datos de famosos en '{NORMALIZED_TABLE_FAMOSOS}' dentro de '{DATABASE_NAME_FAMOSOS}'...")
    conn = None # Inicializar conn a None
    try:
        conn = sqlite3.connect(DATABASE_NAME_FAMOSOS)
        cursor = conn.cursor()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {NORMALIZED_TABLE_FAMOSOS} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT,
            fecha_nacimiento TEXT,
            edad INTEGER,
            cumple_hoy INTEGER
        )
        """)

        # Paso 9: Insertar datos en la tabla
        for _, row in df.iterrows():
            # Solo insertar si la fecha de nacimiento normalizada no es None
            if row['fecha_nacimiento'] is not None:
                cursor.execute(f"""
                INSERT INTO {NORMALIZED_TABLE_FAMOSOS} (nombre, fecha_nacimiento, edad, cumple_hoy)
                VALUES (?, ?, ?, ?)
                """, (row['nombre'], row['fecha_nacimiento'], row['edad'], row['cumple_hoy']))
        
        conn.commit()
        print("✅ Datos de famosos cargados exitosamente en SQLite.")

        # Verificación final de datos cargados
        print(f"\n📊 Contenido de la tabla normalizada de famosos ({NORMALIZED_TABLE_FAMOSOS}):")
        df_check = pd.read_sql_query(f"SELECT * FROM {NORMALIZED_TABLE_FAMOSOS}", conn)
        print(df_check)

    except sqlite3.Error as e:
        print(f"❌ Error de base de datos al cargar datos de famosos: {e}")
    except Exception as e:
        print(f"❌ Error general al cargar datos de famosos: {e}")
    finally:
        if conn:
            conn.close()

    print("--- PROCESO ETL DE FAMOSOS FINALIZADO ---\n")

# --- Código de demostración (se ejecuta solo si este archivo es el principal) ---
if __name__ == "__main__":
    run_etl_famosos()
