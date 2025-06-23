Instalar dependencias:

pip install customtkinter pandas sqlalchemy openpyxl

Usa pandas para manejar datos, SQLAlchemy como conexión futura con SQLite, y CustomTkinter para una interfaz gráfica moderna de escritorio.

Una vez instaladas las dependencias, ejecuta la aplicación con:

python app.py

Esto abrirá la interfaz de usuario con pestañas para ejecutar los distintos procesos ETL disponibles.

En caso de que los archivos de entrada (como DATOS2.txt o DATOS3.txt) no existan, los scripts mostrarán un mensaje de advertencia y crearán archivos de ejemplo automáticamente.

--------------------------------------------------------
CAMBIOS Y DEPENDENCIAS ACTUALIZADAS
--------------------------------------------------------

DEPENDENCIAS:

- customtkinter: Biblioteca para interfaz gráfica de escritorio moderna.
- pandas: Manipulación y transformación de datos.
- sqlalchemy: (Opcional, actualmente no se usa directamente, pero puede incorporarse).
- sqlite3: (Incluido con Python) Gestión de bases de datos locales.
- re: (Incluido) Para validaciones y expresiones regulares.
- os: (Incluido) Para operaciones del sistema de archivos.
- sys: (Incluido) Para redirección de salida estándar.
- io: (Incluido) Para capturar y mostrar salidas ETL dentro de la GUI.
- threading: (Incluido) Para ejecutar procesos sin congelar la ventana.
- openpyxl: Para exportar datos a archivos de Excel (`.xlsx`).

--------------------------------------------------------
CAMBIOS RECIENTES Y FUNCIONALIDADES AGREGADAS
--------------------------------------------------------

- Se implementó una interfaz gráfica moderna usando CustomTkinter, reemplazando el uso de consola.
- El archivo principal `app.py` permite:
  - Ejecutar procesos ETL para ciudades, famosos y ubicaciones desde pestañas dedicadas.
  - Visualizar el log del proceso ETL en tiempo real dentro de la aplicación.
  - Visualizar y exportar el contenido de las bases de datos `.db` generadas por los procesos ETL.
- Se agregó una pestaña de visualización integrada:
  - Permite seleccionar cualquier archivo `.db` desde el sistema de archivos.
  - Detecta automáticamente las tablas y muestra su contenido en la interfaz.
  - Permite exportar tablas a CSV (con punto y coma como separador) y a Excel (`.xlsx`), usando pandas y openpyxl.
- Los procesos ETL están separados por tipo de dato:
  - `etl_ciudades.py` procesa ciudades y crea `ciudades.db`.
  - `etl_famosos.py` procesa famosos y crea `datos_famosos.db`.
  - `etl_ubicacion.py` procesa ubicaciones y crea `datos_ubicacion.db`.
- Se mejoró la normalización de datos:
  - Eliminación de duplicados en los datos de entrada.
  - Corrección y unificación de formatos de fecha.
  - Separación y validación de direcciones y georeferencias.
- Si los archivos de entrada (`DATOS2.txt`, `DATOS3.txt`) no existen, los scripts muestran un mensaje de advertencia y generan archivos de ejemplo automáticamente para facilitar pruebas y uso inicial.
- Se agregó soporte para exportar tablas desde la interfaz gráfica usando pandas, permitiendo elegir el formato y el nombre del archivo de salida.
- Se agregó soporte para exportar a Excel (`.xlsx`) usando la librería `openpyxl`.
- Se mejoró la robustez del código usando hilos (`threading`) para que la interfaz no se congele durante la ejecución de los procesos ETL.
- Se documentó el proceso de instalación y ejecución en este archivo README.

--------------------------------------------------------
EJECUCIÓN
--------------------------------------------------------

1. Asegúrate de tener todos los archivos en el mismo directorio:
   - app.py (interfaz principal)
   - etl_ciudades.py
   - etl_famosos.py
   - etl_ubicacion.py

2. Ejecuta la aplicación con:

   python app.py

3. Interactúa con la interfaz:
   - Sube o genera archivos de entrada
   - Ejecuta los procesos ETL
   - Visualiza los logs y bases de datos generadas

--------------------------------------------------------
RECOMENDACIONES
--------------------------------------------------------

- Para revisar los `.db` fuera de la app, puedes usar DB Browser for SQLite o la extensión SQLite Viewer en Visual Studio Code.
- Asegúrate de tener al menos un archivo DATOS2.txt o DATOS3.txt si no quieres que se genere automáticamente un ejemplo.

