import pandas as pd
from datetime import datetime
import re
import sqlite3
import os # Importar el módulo os para manejar archivos

# --- Configuración de archivos y base de datos ---
INPUT_FILE_FAMOSOS = 'DATOS2.txt'
DATABASE_NAME_FAMOSOS = 'datos_famosos.db'
NORMALIZED_TABLE_FAMOSOS = 'fnac_famosos_norm'

# Función principal que ejecuta el proceso ETL para famosos
def run_etl_famosos():
    """
    Ejecuta el proceso ETL (Extracción, Transformación, Carga) para los datos de famosos.
    Normaliza nombres y fechas, calcula edad y flag de cumpleaños, elimina duplicados,
    y carga los datos procesados en una base de datos SQLite.
    """
    print("\n--- INICIANDO PROCESO ETL DE FAMOSOS ---")

    # --- Gestión de la base de datos (eliminamos para asegurar consistencia) ---
    if os.path.exists(DATABASE_NAME_FAMOSOS):
        print(f"DEBUG: Eliminando base de datos existente '{DATABASE_NAME_FAMOSOS}'.")
        try:
            os.remove(DATABASE_NAME_FAMOSOS)
            print(f"✅ Base de datos '{DATABASE_NAME_FAMOSOS}' eliminada exitosamente.")
        except OSError as e:
            print(f"❌ Error al eliminar '{DATABASE_NAME_FAMOSOS}': {e}. ¡Asegúrate de que no esté abierto en otra aplicación!")
            print("❌ El proceso ETL de famosos no puede continuar con una base de datos antigua que podría causar inconsistencias.")
            # Si el archivo es crítico y no se puede borrar, podrías salir del programa: sys.exit(1)
            # Por ahora, continuamos para que el usuario vea el mensaje y el proceso falle más abajo si es necesario.
            return # Salir de la función si no se puede eliminar el archivo.

    # --- Paso 1: Leer el archivo como texto plano ---
    # Verificación si el archivo de entrada existe
    if not os.path.exists(INPUT_FILE_FAMOSOS):
        print(f"❌ Error: El archivo '{INPUT_FILE_FAMOSOS}' no fue encontrado. Creando archivo de ejemplo para pruebas...")
        try:
            with open(INPUT_FILE_FAMOSOS, 'w', encoding='utf-8') as f:
                f.write("1. Abraham Lincoln - 12-02-1809\n")
                f.write("2. Albert Einstein - 14-03-1879\n")
                f.write("3. Marie Curie - 07-11-1867\n")
                f.write("4. Isaac Newton - 04-01-1643\n")
                f.write("5. Leonardo da Vinci - 15-04-1452\n")
                f.write("6. Albert Einstein - 14-03-1879\n") # Duplicado
                f.write("7. Abraham Lincoln - 12-02-1809\n") # Duplicado
                f.write("8. Rosa Parks - 04/02/1913\n") # Formato de fecha diferente
                f.write("9. Charles Darwin - 1809-02-12\n") # Formato de fecha diferente
                f.write("10. Ada Lovelace - 10.12.1815\n") # Formato de fecha diferente
                f.write("11. Alan Turing - 23-06-1912\n")
                f.write("12. AMELIA EARHART - 24-07-1897\n") # Mismas mayúsculas/minúsculas
                f.write("13. Amelia Earhart - 24-07-1897\n") # Duplicado por mayúsculas/minúsculas
                f.write("14. ANNE FRANK - 12-06-1929\n") # Duplicado por mayúsculas/minúsculas
                f.write("15. Anne Frank - 12-06-1929\n") # Duplicado por mayúsculas/minúsculas
                f.write("16. Nelson Mandela - 18-07-1918\n")
                f.write("17. Nelson Mandela - 18/07/1918\n") # Duplicado con fecha diferente
            print(f"✅ Archivo '{INPUT_FILE_FAMOSOS}' creado exitosamente en {os.getcwd()}.")
        except Exception as e:
            print(f"❌ Error al crear el archivo de ejemplo '{INPUT_FILE_FAMOSOS}': {e}")
            print("❌ El proceso ETL de famosos no puede continuar sin el archivo de entrada.")
            return # Salir de la función si no se puede crear el archivo de ejemplo.
    else:
        print(f"ℹ️ Archivo '{INPUT_FILE_FAMOSOS}' encontrado. Usando archivo existente.")

    try:
        with open(INPUT_FILE_FAMOSOS, "r", encoding="utf-8") as file:
            lineas = file.readlines()
        print(f"✅ Datos de famosos extraídos exitosamente desde '{INPUT_FILE_FAMOSOS}'.")
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{INPUT_FILE_FAMOSOS}' no fue encontrado.")
        return # Salir de la función si el archivo no existe.
    except Exception as e:
        print(f"❌ Error al leer el archivo '{INPUT_FILE_FAMOSOS}': {e}")
        return # Salir de la función si hay un error de lectura.

    # Paso 2: Extraer nombre y fecha de cada línea
    data = []
    for linea in lineas:
        linea = re.sub(r'^\d+\.\s*', '', linea.strip()) # Elimina números al inicio y espacios.
        if '-' in linea: # Si la línea contiene un guion, asumimos que es un separador nombre-fecha.
            partes = linea.split('-', 1) # Divide la línea en dos partes: nombre y fecha.
            nombre = partes[0].strip() # Limpia espacios alrededor del nombre.
            fecha = partes[1].strip() # Limpia espacios alrededor de la fecha.
            data.append((nombre, fecha))

    # Paso 3: Crear DataFrame
    df = pd.DataFrame(data, columns=["nombre", "fecha_nacimiento_raw"])
    print(f"DEBUG: DataFrame inicial creado con {len(df)} filas.")
    print("DEBUG: Primeras filas del DataFrame inicial:")
    print(df.head().to_string(index=False)) # Imprime sin el índice de Pandas

    # Paso 4: Normalizar fecha
    def transformar_fecha(fecha_raw):
        """
        Intenta transformar una cadena de fecha en varios formatos a un formato estándar 'DD-MM-YYYY'.
        Devuelve None si no puede transformar la fecha o si contiene palabras como 'alrededor' o 'a.c.'.
        """
        if not isinstance(fecha_raw, str): # Asegurarse de que es una cadena.
            return None
        fecha_raw = fecha_raw.lower() # Convertir a minúsculas para manejar "Alrededor" o "A.C.".
        if "alrededor" in fecha_raw or "a.c" in fecha_raw:
            return None # Ignorar fechas aproximadas o antes de Cristo.

        # Reemplazar diferentes separadores por guiones.
        fecha_raw = fecha_raw.replace(".", "-").replace("/", "-")

        # Intentar varios formatos comunes de fecha.
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d-%m-%y", "%Y%m%d"): # Añadido %d-%m-%y para años de 2 dígitos, %Y%m%d para sin separadores.
            try:
                fecha = datetime.strptime(fecha_raw, fmt) # Intenta parsear la fecha con el formato actual.
                return fecha.strftime("%d-%m-%Y") # Devuelve la fecha en el formato estándar deseado.
            except ValueError: # Si el formato no coincide, pasa al siguiente.
                continue
        return None # Si ningún formato coincide, devuelve None.

    df['fecha_nacimiento'] = df['fecha_nacimiento_raw'].apply(transformar_fecha)
    print("\nDEBUG: DataFrame después de normalizar fechas:")
    print(df[['nombre', 'fecha_nacimiento']].head().to_string(index=False))

    # Eliminar filas donde la fecha de nacimiento es None (no se pudo normalizar)
    initial_rows_after_date_norm = len(df)
    df.dropna(subset=['fecha_nacimiento'], inplace=True)
    if len(df) < initial_rows_after_date_norm:
        print(f"  - Se eliminaron {initial_rows_after_date_norm - len(df)} filas con fechas de nacimiento inválidas.")

    # Paso 5: Calcular edad
    def calcular_edad(fecha_str):
        """
        Calcula la edad de una persona basándose en su fecha de nacimiento.
        Devuelve None si la fecha no es válida.
        """
        if fecha_str:
            try:
                fecha = datetime.strptime(fecha_str, "%d-%m-%Y")
                hoy = datetime.now()
                # Calcula la edad: diferencia de años - 1 si el cumpleaños aún no ha pasado este año.
                return hoy.year - fecha.year - ((hoy.month, hoy.day) < (fecha.month, fecha.day))
            except ValueError:
                return None # Si la fecha no está en el formato esperado, devuelve None.
        return None

    df['edad'] = df['fecha_nacimiento'].apply(calcular_edad)

    # Paso 6: Flag cumpleaños
    def cumple_hoy(fecha_str):
        """
        Verifica si una fecha de nacimiento es hoy.
        Devuelve 1 si es el cumpleaños hoy, 0 en caso contrario.
        """
        if fecha_str:
            try:
                fecha = datetime.strptime(fecha_str, "%d-%m-%Y")
                hoy = datetime.now()
                # Compara solo el día y el mes.
                return int((fecha.day == hoy.day) and (fecha.month == hoy.month))
            except ValueError:
                return 0 # Si la fecha no es válida, no es cumpleaños.
        return 0

    df['cumple_hoy'] = df['fecha_nacimiento'].apply(cumple_hoy)

    # --- NORMALIZACIÓN ADICIONAL PARA LA DEDUPLICACIÓN ---
    # Convertir 'nombre' a mayúsculas y eliminar espacios extra (si los hubiera)
    df['nombre'] = df['nombre'].astype(str).str.strip().str.upper()
    print("\nDEBUG: DataFrame después de normalizar nombres a MAYÚSCULAS y eliminar espacios:")
    print(df[['nombre', 'fecha_nacimiento']].head(10).to_string(index=False)) # Muestra más filas para ver duplicados

    # Paso 7: Eliminar duplicados por nombre y fecha
    # Ahora, la eliminación de duplicados debería ser más efectiva gracias a la normalización de 'nombre'.
    rows_before_dedup = len(df)
    df = df.drop_duplicates(subset=['nombre', 'fecha_nacimiento'])
    rows_after_dedup = len(df)

    if rows_before_dedup > rows_after_dedup:
        print(f"✅ Se eliminaron {rows_before_dedup - rows_after_dedup} filas duplicadas de la tabla de famosos.")
    else:
        print("ℹ️ No se encontraron duplicados en la tabla de famosos para eliminar.")

    print("\nDEBUG: DataFrame final después de eliminar duplicados:")
    print(df.head(10).to_string(index=False)) # Imprime las primeras 10 filas del DataFrame final
    print(f"DEBUG: DataFrame final tiene {len(df)} filas.")


    # Paso 8: Conectar a SQLite y crear tabla
    # Usamos 'datos_famosos.db' para el ETL de famosos, separado de otras DBs.
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
    print(f"✅ Tabla '{NORMALIZED_TABLE_FAMOSOS}' creada o verificada en '{DATABASE_NAME_FAMOSOS}'.")


    # Paso 9: Insertar datos en la tabla
    inserted_count = 0
    for index, row in df.iterrows():
        # Solo insertamos si la fecha_nacimiento no es None (porque esas filas ya fueron dropeadas pero por si acaso).
        if pd.notna(row['fecha_nacimiento']):
            cursor.execute(f"""
            INSERT INTO {NORMALIZED_TABLE_FAMOSOS} (nombre, fecha_nacimiento, edad, cumple_hoy)
            VALUES (?, ?, ?, ?)
            """, (row['nombre'], row['fecha_nacimiento'], row['edad'], row['cumple_hoy']))
            inserted_count += 1

    conn.commit() # Guarda los cambios en la base de datos.
    conn.close() # Cierra la conexión a la base de datos.

    print(f"✅ Datos insertados en SQLite correctamente. {inserted_count} filas insertadas.")

    # --- Verificación final (opcional) ---
    try:
        conn_check = sqlite3.connect(DATABASE_NAME_FAMOSOS)
        df_check = pd.read_sql_table(NORMALIZED_TABLE_FAMOSOS, con=conn_check)
        print(f"\n📊 Contenido de la tabla '{NORMALIZED_TABLE_FAMOSOS}' después de la carga:")
        print(df_check.to_string(index=False)) # Imprime la tabla completa en consola
        conn_check.close()
    except Exception as e:
        print(f"❌ Error al leer la tabla para verificación: {e}")

    print("\n--- PROCESO ETL DE FAMOSOS FINALIZADO ---")

# Si este script se ejecuta directamente, llama a la función ETL.
# Esto es útil para probar el script de forma independiente.
if __name__ == "__main__":
    run_etl_famosos()
