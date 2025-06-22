Instalar dependencias:

pip install customtkinter pandas sqlalchemy

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

--------------------------------------------------------
CAMBIOS RECIENTES
--------------------------------------------------------

- Interfaz gráfica creada con CustomTkinter, reemplazando el uso de consola.
- El archivo principal `app.py` permite al usuario:
  - Ejecutar procesos ETL (ciudades, famosos, ubicaciones).
  - Visualizar el log del proceso en tiempo real.
  - Visualizar archivos `.db` generados.
- Pestaña de visualización integrada:
  - Permite seleccionar archivos `.db` desde el sistema.
  - Detecta tablas y muestra su contenido.
- ETL separados por tipo:
  - `etl_ciudades.py` → ciudades
  - `etl_famosos.py` → famosos
  - `etl_ubicacion.py` → lugares y geolocalización
- Normalización completa:
  - Eliminación de duplicados
  - Corrección de formatos de fecha
  - Separación y validación de direcciones
- Los scripts crean archivos `.db` independientes:
  - `ciudades.db`, `datos_famosos.db`, `datos_ubicacion.db`

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

