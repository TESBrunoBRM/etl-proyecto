# Importaciones necesarias para la interfaz y manejo de archivos, hilos y bases de datos
import customtkinter as ctk
from tkinter import messagebox, filedialog, ttk
import sys
import io
import os
import threading
import sqlite3
import pandas as pd
from datetime import datetime # Para generar nombres de archivo únicos en la exportación
import shutil # Importar shutil para copiar archivos

# Importar funciones ETL de los módulos correspondientes
try:
    from etl_ciudades import run_etl_ciudades
    from etl_famosos import run_etl_famosos
    # Solo importamos run_etl_ubicacion, no las funciones de exportación o constantes específicas
    from etl_ubicacion import run_etl_ubicacion
except ImportError as e:
    messagebox.showerror("Error de Importación",
                         f"No se pudieron cargar los módulos ETL: {e}\n"
                         "Asegúrate de que 'etl_ciudades.py', 'etl_famosos.py' y 'etl_ubicacion.py' "
                         "estén en el mismo directorio que 'app.py'.")
    sys.exit(1)

class EtlApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Configuración de la ventana principal
        self.title("Aplicación de Procesos ETL & Visor DB")
        self.geometry("1200x800")
        self.resizable(True, True)

        # Definición de la paleta de colores personalizada
        self.BG_PRIMARY = "#1A1A2E"
        self.BG_SECONDARY = "#2C2C40"
        self.ACCENT_PRIMARY = "#6F42C1"
        self.ACCENT_HOVER = "#8A6CCF"
        self.TEXT_COLOR = "#E0E0E0"
        self.BORDER_COLOR = "#4A4A60"

        # Configuración del tema de CustomTkinter
        ctk.set_appearance_mode("Dark")
        ctk.set_default_color_theme("blue")
        ctk.set_widget_scaling(1.1)

        # Intentar cargar el icono de la aplicación
        try:
            self.iconbitmap(os.path.join(os.path.dirname(__file__), "app_icon.ico"))
        except ctk.TclError:
            print("Advertencia: No se pudo cargar el icono de la aplicación. Asegúrate de que 'app_icon.ico' existe o especifica la ruta correcta.")
            print("Además, verifica que el archivo .ico contenga múltiples resoluciones para una mejor compatibilidad con la barra de tareas.")

        self.current_db_path = None

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # Crear el Tabview principal con tres pestañas: Procesos ETL, Visualizar DB y Descarga de Archivos
        self.tab_view = ctk.CTkTabview(self,
                                       width=1180,
                                       height=780,
                                       corner_radius=15,
                                       fg_color=self.BG_SECONDARY,
                                       segmented_button_selected_color=self.ACCENT_PRIMARY,
                                       segmented_button_selected_hover_color=self.ACCENT_HOVER,
                                       segmented_button_unselected_color=self.BG_SECONDARY,
                                       segmented_button_unselected_hover_color=self.BORDER_COLOR,
                                       text_color=self.TEXT_COLOR)
        self.tab_view.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

        # --- Pestaña para Procesos ETL ---
        self.tab_view.add("Procesos ETL")
        tab_etl = self.tab_view.tab("Procesos ETL")
        tab_etl.grid_rowconfigure(0, weight=0)
        tab_etl.grid_rowconfigure(1, weight=0)
        tab_etl.grid_rowconfigure(2, weight=0)
        tab_etl.grid_rowconfigure(3, weight=1)
        tab_etl.grid_columnconfigure((0,1,2), weight=1)

        # Título de la pestaña ETL
        title_label = ctk.CTkLabel(tab_etl, text="✨ Aplicación de Procesos ETL ✨",
                                   font=ctk.CTkFont(family="Arial", size=28, weight="bold"),
                                   text_color=self.TEXT_COLOR)
        title_label.grid(row=0, column=0, columnspan=3, pady=20)

        # Frame para los botones de procesos ETL
        process_frame = ctk.CTkFrame(tab_etl, corner_radius=15, fg_color=self.BG_SECONDARY, border_color=self.BORDER_COLOR, border_width=2)
        process_frame.grid(row=1, column=0, columnspan=3, pady=15, padx=30, sticky="ew")
        process_frame.grid_columnconfigure((0,1,2), weight=1)

        # Configuración de botones
        button_font = ctk.CTkFont(family="Arial", size=15, weight="bold")
        button_height = 45
        button_radius = 12

        # Botón para proceso ETL de ciudades
        self.btn_ciudades = ctk.CTkButton(process_frame, text="🏢 Proceso Ciudades",
                                          command=lambda: self.run_etl_process("Ciudades"),
                                          height=button_height, corner_radius=button_radius,
                                          font=button_font, fg_color=self.ACCENT_PRIMARY, hover_color=self.ACCENT_HOVER,
                                          text_color=self.TEXT_COLOR)
        self.btn_ciudades.grid(row=0, column=0, padx=15, pady=10, sticky="ew")

        # Botón para proceso ETL de famosos
        self.btn_famosos = ctk.CTkButton(process_frame, text="🌟 Proceso Famosos",
                                         command=lambda: self.run_etl_process("Famosos"),
                                         height=button_height, corner_radius=button_radius,
                                         font=button_font, fg_color=self.ACCENT_PRIMARY, hover_color=self.ACCENT_HOVER,
                                         text_color=self.TEXT_COLOR)
        self.btn_famosos.grid(row=0, column=1, padx=15, pady=10, sticky="ew")

        # Botón para proceso ETL de ubicación
        self.btn_ubicacion = ctk.CTkButton(process_frame, text="📌 Proceso Ubicación",
                                           command=lambda: self.run_etl_process("Ubicacion"),
                                           height=button_height, corner_radius=button_radius,
                                           font=button_font, fg_color=self.ACCENT_PRIMARY, hover_color=self.ACCENT_HOVER,
                                           text_color=self.TEXT_COLOR)
        self.btn_ubicacion.grid(row=0, column=2, padx=15, pady=10, sticky="ew")

        # Barra de progreso para los procesos ETL
        self.progress_bar = ctk.CTkProgressBar(tab_etl, orientation="horizontal", height=12, corner_radius=8,
                                                fg_color=self.BORDER_COLOR, progress_color=self.ACCENT_PRIMARY)
        self.progress_bar.grid(row=2, column=0, columnspan=3, padx=30, pady=10, sticky="ew")
        self.progress_bar.set(0)

        # Área de logs para mostrar la salida de los procesos ETL
        self.output_log = ctk.CTkTextbox(tab_etl, wrap="word",
                                        font=ctk.CTkFont(family="Consolas", size=12),
                                        corner_radius=10, fg_color=self.BG_SECONDARY,
                                        text_color=self.TEXT_COLOR, border_color=self.BORDER_COLOR, border_width=2)
        self.output_log.grid(row=3, column=0, columnspan=3, padx=30, pady=15, sticky="nsew")
        self.output_log.insert("end", "Esperando la selección de un proceso ETL...\n")
        self.output_log.configure(state="disabled")

        # Redirigir stdout a la caja de logs
        self.original_stdout = sys.stdout
        sys.stdout = self

        self.protocol("WM_DELETE_WINDOW", self.on_closing)

        # --- Pestaña para Visualizar DB ---
        self.tab_view.add("Visualizar DB")
        tab_db = self.tab_view.tab("Visualizar DB")
        tab_db.grid_rowconfigure(0, weight=0) # Controles de DB
        tab_db.grid_rowconfigure(1, weight=0) # Controles de Exportación
        tab_db.grid_rowconfigure(2, weight=1) # Treeview
        tab_db.grid_columnconfigure(0, weight=1)

        # Frame de controles para seleccionar y abrir bases de datos
        db_controls_frame = ctk.CTkFrame(tab_db, corner_radius=15, fg_color=self.BG_SECONDARY, border_color=self.BORDER_COLOR, border_width=2)
        db_controls_frame.grid(row=0, column=0, padx=30, pady=15, sticky="ew")
        db_controls_frame.grid_columnconfigure((0,1,2,3,4,5), weight=1)

        label_font = ctk.CTkFont(family="Arial", size=14, weight="bold")
        option_menu_font = ctk.CTkFont(family="Arial", size=13)
        small_button_height = 35
        small_button_radius = 10

        # Selector de base de datos
        ctk.CTkLabel(db_controls_frame, text="Seleccionar DB:", font=label_font, text_color=self.TEXT_COLOR).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.db_selector = ctk.CTkOptionMenu(db_controls_frame, values=["(Sin DBs encontradas)"], command=self.load_tables,
                                             font=option_menu_font, fg_color=self.BG_SECONDARY, button_color=self.ACCENT_PRIMARY,
                                             button_hover_color=self.ACCENT_HOVER, dropdown_fg_color=self.BG_SECONDARY,
                                             dropdown_hover_color=self.BORDER_COLOR, text_color=self.TEXT_COLOR,
                                             corner_radius=small_button_radius)
        self.db_selector.grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        
        # Botón para actualizar la lista de bases de datos
        self.refresh_db_list_button = ctk.CTkButton(db_controls_frame, text="🔄 Actualizar DBs", command=self.populate_db_selector,
                                                    height=small_button_height, corner_radius=small_button_radius,
                                                    font=button_font, fg_color=self.ACCENT_PRIMARY, hover_color=self.ACCENT_HOVER,
                                                    text_color=self.TEXT_COLOR)
        self.refresh_db_list_button.grid(row=0, column=2, padx=10, pady=5, sticky="ew")
        
        # Botón para abrir un archivo de base de datos manualmente
        self.open_db_dialog_button = ctk.CTkButton(db_controls_frame, text="📁 Abrir DB...", command=self.open_db_file_dialog,
                                                  height=small_button_height, corner_radius=small_button_radius,
                                                  font=button_font, fg_color=self.ACCENT_PRIMARY, hover_color=self.ACCENT_HOVER,
                                                  text_color=self.TEXT_COLOR)
        self.open_db_dialog_button.grid(row=0, column=3, padx=10, pady=5, sticky="ew")

        # Selector de tabla dentro de la base de datos
        ctk.CTkLabel(db_controls_frame, text="Seleccionar Tabla:", font=label_font, text_color=self.TEXT_COLOR).grid(row=0, column=4, padx=10, pady=5, sticky="w")
        self.table_selector = ctk.CTkOptionMenu(db_controls_frame, values=["(Seleccione una DB primero)"], command=self.display_table_content,
                                                font=option_menu_font, fg_color=self.BG_SECONDARY, button_color=self.ACCENT_PRIMARY,
                                                button_hover_color=self.ACCENT_HOVER, dropdown_fg_color=self.BG_SECONDARY,
                                                dropdown_hover_color=self.BORDER_COLOR, text_color=self.TEXT_COLOR,
                                                corner_radius=small_button_radius)
        self.table_selector.grid(row=0, column=5, padx=10, pady=5, sticky="ew")

        # --- Frame para las opciones de exportación de tabla ---
        export_frame = ctk.CTkFrame(tab_db, corner_radius=15, fg_color=self.BG_SECONDARY, border_color=self.BORDER_COLOR, border_width=2)
        export_frame.grid(row=1, column=0, padx=30, pady=15, sticky="ew")
        export_frame.grid_columnconfigure((0,1,2), weight=1)

        ctk.CTkLabel(export_frame, text="Exportar Tabla Actual:", font=label_font, text_color=self.TEXT_COLOR).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        
        self.btn_export_csv = ctk.CTkButton(export_frame, text="⬇️ Exportar CSV", 
                                            command=lambda: self.export_selected_table_threaded('csv'),
                                            height=small_button_height, corner_radius=small_button_radius,
                                            font=button_font, fg_color="#28a745", hover_color="#218838",
                                            text_color=self.TEXT_COLOR)
        self.btn_export_csv.grid(row=0, column=1, padx=10, pady=5, sticky="ew")

        self.btn_export_excel = ctk.CTkButton(export_frame, text="⬇️ Exportar Excel", 
                                              command=lambda: self.export_selected_table_threaded('excel'),
                                              height=small_button_height, corner_radius=small_button_radius,
                                              font=button_font, fg_color="#007bff", hover_color="#0069d9",
                                              text_color=self.TEXT_COLOR)
        self.btn_export_excel.grid(row=0, column=2, padx=10, pady=5, sticky="ew")
        # Por defecto deshabilitar los botones de exportación hasta que se seleccione una tabla válida
        self.set_export_buttons_state("disabled")

        # --- Treeview para mostrar el contenido de la tabla ---
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview",
                        background=self.BG_SECONDARY,
                        foreground=self.TEXT_COLOR,
                        fieldbackground=self.BG_SECONDARY,
                        bordercolor=self.BORDER_COLOR,
                        lightcolor=self.BG_SECONDARY,
                        darkcolor=self.BG_PRIMARY,
                        rowheight=28)
        style.map('Treeview',
                  background=[('selected', self.ACCENT_PRIMARY)],
                  foreground=[('selected', 'white')])
        style.configure("Treeview.Heading",
                        background=self.ACCENT_PRIMARY,
                        foreground="white",
                        font=('Arial', 12, 'bold'),
                        borderwidth=0,
                        relief="flat")
        style.map("Treeview.Heading",
                  background=[('active', self.ACCENT_HOVER)])

        # Estilo personalizado para Scrollbar
        style.element_create("Vertical.TScrollbar.trough", "from", "clam")
        style.element_create("Horizontal.TScrollbar.trough", "from", "clam")
        style.configure("Vertical.TScrollbar",
                        troughcolor=self.BG_PRIMARY,
                        background=self.BORDER_COLOR,
                        bordercolor=self.BG_PRIMARY,
                        arrowsize=18,
                        arrowcolor=self.TEXT_COLOR,
                        relief="flat",
                        gripcount=0,
                        griprelief="flat",
                        padding=0)
        style.map("Vertical.TScrollbar",
                  background=[('active', self.ACCENT_PRIMARY)],
                  arrowcolor=[('active', "white")])
        style.configure("Horizontal.TScrollbar",
                        troughcolor=self.BG_PRIMARY,
                        background=self.BORDER_COLOR,
                        bordercolor=self.BG_PRIMARY,
                        arrowsize=18,
                        arrowcolor=self.TEXT_COLOR,
                        relief="flat",
                        gripcount=0,
                        griprelief="flat",
                        padding=0)
        style.map("Horizontal.TScrollbar",
                  background=[('active', self.ACCENT_PRIMARY)],
                  arrowcolor=[('active', "white")])

        # Frame para el Treeview
        self.tree_frame = ctk.CTkFrame(tab_db, corner_radius=15, fg_color="transparent")
        self.tree_frame.grid(row=2, column=0, padx=30, pady=15, sticky="nsew")
        self.tree_frame.grid_rowconfigure(0, weight=1)
        self.tree_frame.grid_columnconfigure(0, weight=1)

        # Treeview para mostrar los datos de la tabla seleccionada
        self.db_treeview = ttk.Treeview(self.tree_frame, show="headings")
        self.db_treeview.grid(row=0, column=0, sticky="nsew")

        # Scrollbars para el Treeview
        vsb = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.db_treeview.yview, style="Vertical.TScrollbar")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.db_treeview.xview, style="Horizontal.TScrollbar")
        hsb.grid(row=1, column=0, sticky="ew")
        self.db_treeview.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # --- Nueva Pestaña para Descarga de Archivos ---
        self.tab_view.add("Descarga de Archivos")
        tab_download = self.tab_view.tab("Descarga de Archivos")
        tab_download.grid_rowconfigure(0, weight=0)
        tab_download.grid_rowconfigure(1, weight=1)
        tab_download.grid_columnconfigure(0, weight=1)
        tab_download.grid_columnconfigure(1, weight=1)


        download_controls_frame = ctk.CTkFrame(tab_download, corner_radius=15, fg_color=self.BG_SECONDARY, border_color=self.BORDER_COLOR, border_width=2)
        download_controls_frame.grid(row=0, column=0, columnspan=2, padx=30, pady=15, sticky="ew")
        download_controls_frame.grid_columnconfigure((0,1,2), weight=1)

        ctk.CTkLabel(download_controls_frame, text="Seleccionar archivo a descargar:", font=label_font, text_color=self.TEXT_COLOR).grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.file_download_selector = ctk.CTkOptionMenu(download_controls_frame, values=["(No se encontraron archivos)"],
                                                        font=option_menu_font, fg_color=self.BG_SECONDARY, button_color=self.ACCENT_PRIMARY,
                                                        button_hover_color=self.ACCENT_HOVER, dropdown_fg_color=self.BG_SECONDARY,
                                                        dropdown_hover_color=self.BORDER_COLOR, text_color=self.TEXT_COLOR,
                                                        corner_radius=small_button_radius)
        self.file_download_selector.grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        
        self.btn_download_file = ctk.CTkButton(download_controls_frame, text="⬇️ Descargar Archivo", 
                                               command=self.download_selected_file_threaded,
                                               height=small_button_height, corner_radius=small_button_radius,
                                               font=button_font, fg_color="#6c757d", hover_color="#5a6268",
                                               text_color=self.TEXT_COLOR)
        self.btn_download_file.grid(row=0, column=2, padx=10, pady=5, sticky="ew")
        self.btn_download_file.configure(state="disabled")

        # Llenar los selectores al iniciar
        self.populate_db_selector()
        self.populate_download_file_selector()


    def write(self, text):
        self.output_log.configure(state="normal")
        self.output_log.insert("end", text)
        self.output_log.see("end")
        self.output_log.configure(state="disabled")
        self.update_idletasks()

    def flush(self):
        pass

    def on_closing(self):
        sys.stdout = self.original_stdout
        self.destroy()

    def set_buttons_state(self, state):
        self.btn_ciudades.configure(state=state)
        self.btn_famosos.configure(state=state)
        self.btn_ubicacion.configure(state=state)

    def set_export_buttons_state(self, state):
        self.btn_export_csv.configure(state=state)
        self.btn_export_excel.configure(state=state)

    def set_download_button_state(self, state):
        self.btn_download_file.configure(state=state)

    def run_etl_process(self, process_name):
        self.output_log.configure(state="normal")
        self.output_log.delete("1.0", "end")
        self.output_log.configure(state="disabled")
        self.write(f"--- INICIANDO PROCESO ETL DE {process_name.upper()} ---\n")

        self.set_buttons_state("disabled")
        self.progress_bar.start()

        thread = threading.Thread(target=self._execute_etl_thread, args=(process_name,))
        thread.start()

    def _execute_etl_thread(self, process_name):
        try:
            if process_name == "Ciudades":
                run_etl_ciudades()
            elif process_name == "Famosos":
                run_etl_famosos()
            elif process_name == "Ubicacion":
                run_etl_ubicacion() # Esta es la función que genera datos_ubicacion.db
            else:
                self.write(f"❌ Error: Proceso ETL '{process_name}' no reconocido.\n")
                self.after(0, lambda: messagebox.showerror("Error en Proceso ETL", f"Proceso ETL '{process_name}' no reconocido."))
                return
            
            self.write(f"\n--- PROCESO ETL DE {process_name.upper()} FINALIZADO ---\n")
            self.after(0, lambda: messagebox.showinfo("Proceso Completado", f"El proceso ETL de {process_name} ha finalizado exitosamente."))

        except Exception as e:
            self.write(f"\n❌ ERROR CRÍTICO DURANTE EL PROCESO ETL DE {process_name.upper()}: {e}\n")
            self.after(0, lambda: messagebox.showerror("Error en Proceso ETL", f"Ocurrió un error durante el proceso ETL de {process_name}:\n{e}"))
        finally:
            self.after(0, lambda: self.set_buttons_state("normal"))
            self.after(0, self.progress_bar.stop)
            self.after(0, self.populate_db_selector)
            self.after(0, self.populate_download_file_selector) # Actualizar lista de archivos descargables

    # Esta función ahora manejará la exportación de la tabla actual desde app.py
    def export_selected_table_threaded(self, file_format):
        selected_db_name = self.db_selector.get()
        selected_table_name = self.table_selector.get()

        if not self.current_db_path or not os.path.exists(self.current_db_path):
            messagebox.showwarning("Advertencia de Exportación", "Por favor, seleccione una base de datos válida primero.")
            self.write("⚠️ Advertencia: No se puede exportar. Seleccione una DB válida.\n")
            return
        
        if selected_table_name in ["(Seleccione una DB primero)", "(No hay tablas)", "(Error al cargar tablas)"]:
            messagebox.showwarning("Advertencia de Exportación", "Por favor, seleccione una tabla válida para exportar.")
            self.write("⚠️ Advertencia: No se puede exportar. Seleccione una tabla válida.\n")
            return

        self.write(f"\n--- INICIANDO EXPORTACIÓN DE TABLA '{selected_table_name}' de '{selected_db_name}' a {file_format.upper()} ---\n")
        self.set_export_buttons_state("disabled")

        export_thread = threading.Thread(target=self._execute_export_table_logic, 
                                         args=(self.current_db_path, selected_table_name, file_format))
        export_thread.start()

    def _execute_export_table_logic(self, db_path, table_name, file_format):
        """
        Lógica interna para exportar una tabla específica a CSV o Excel.
        Esta lógica vive directamente en app.py ahora.
        """
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            # Verificar si la tabla existe
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if cursor.fetchone() is None:
                message = f"❌ Error: La tabla '{table_name}' no existe en la base de datos '{db_path}'."
                self.after(0, lambda: self.write(message + "\n"))
                self.after(0, lambda: messagebox.showerror("Error de Exportación", message))
                return

            df_export = pd.read_sql_query(f"SELECT * FROM \"{table_name}\"", conn)

            if df_export.empty:
                message = f"⚠️ Advertencia: La tabla '{table_name}' está vacía. No hay datos para exportar."
                self.after(0, lambda: self.write(message + "\n"))
                self.after(0, lambda: messagebox.showwarning("Tabla Vacía", message))
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Nombre de archivo más claro que incluye la DB y la tabla
            base_db_name = os.path.splitext(os.path.basename(db_path))[0]
            file_name = f"{base_db_name}_{table_name}_export_{timestamp}"

            if file_format.lower() == 'csv':
                output_path = filedialog.asksaveasfilename(
                    initialfile=f"{file_name}.csv",
                    defaultextension=".csv",
                    filetypes=[("Archivos CSV", "*.csv"), ("Todos los archivos", "*.*")]
                )
                if output_path:
                    # CAMBIO: Usar punto y coma como delimitador para CSV
                    df_export.to_csv(output_path, index=False, encoding='utf-8', sep=';')
                    message = f"✅ Tabla '{table_name}' exportada exitosamente a:\n{output_path}"
                    self.after(0, lambda: self.write(message + "\n"))
                    self.after(0, lambda: messagebox.showinfo("Exportación Completada", message))
                else:
                    message = "ℹ️ Exportación a CSV cancelada."
                    self.after(0, lambda: self.write(message + "\n"))
                    self.after(0, lambda: messagebox.showinfo("Exportación Cancelada", message))

            elif file_format.lower() == 'excel':
                output_path = filedialog.asksaveasfilename(
                    initialfile=f"{file_name}.xlsx",
                    defaultextension=".xlsx",
                    filetypes=[("Archivos Excel", "*.xlsx"), ("Todos los archivos", "*.*")]
                )
                if output_path:
                    df_export.to_excel(output_path, index=False)
                    message = f"✅ Tabla '{table_name}' exportada exitosamente a:\n{output_path}"
                    self.after(0, lambda: self.write(message + "\n"))
                    self.after(0, lambda: messagebox.showinfo("Exportación Completada", message))
                else:
                    message = "ℹ️ Exportación a Excel cancelada."
                    self.after(0, lambda: self.write(message + "\n"))
                    self.after(0, lambda: messagebox.showinfo("Exportación Cancelada", message))
            else:
                message = f"❌ Formato de archivo no soportado para exportación de tabla: '{file_format}'. Por favor, elija 'csv' o 'excel'."
                self.after(0, lambda: self.write(message + "\n"))
                self.after(0, lambda: messagebox.showerror("Error de Formato", message))

        except Exception as e:
            error_message = f"❌ ERROR CRÍTICO DURANTE LA EXPORTACIÓN DE TABLA '{table_name}': {e}"
            self.after(0, lambda: self.write(error_message + "\n"))
            self.after(0, lambda: messagebox.showerror("Error de Exportación", error_message))
        finally:
            if conn:
                conn.close()
            self.after(0, lambda: self.set_export_buttons_state("normal"))


    def populate_db_selector(self):
        db_files = [f for f in os.listdir('.') if f.endswith('.db')]
        
        if not db_files:
            self.db_selector.configure(values=["(No se encontraron DBs)"])
            self.db_selector.set("(No se encontraron DBs)")
            self.table_selector.configure(values=["(Seleccione una DB primero)"])
            self.table_selector.set("(Seleccione una DB primero)")
            self.clear_treeview()
            self.set_export_buttons_state("disabled")
            return

        current_selection = self.db_selector.get()
        if current_selection in db_files:
            self.db_selector.configure(values=db_files)
            self.db_selector.set(current_selection)
            self.load_tables(current_selection)
        else:
            self.db_selector.configure(values=db_files)
            self.db_selector.set(db_files[0])
            self.load_tables(db_files[0])
        
        self.set_export_buttons_state("normal")


    def load_tables(self, db_name):
        if db_name == "(No se encontraron DBs)":
            self.table_selector.configure(values=["(Seleccione una DB primero)"])
            self.table_selector.set("(Seleccione una DB primero)")
            self.clear_treeview()
            self.set_export_buttons_state("disabled")
            return

        # Asegurarse de que el path sea completo
        self.current_db_path = os.path.join(os.getcwd(), db_name)
        if not os.path.exists(self.current_db_path):
            self.table_selector.configure(values=["(Error al cargar DB)"])
            self.table_selector.set("(Error al cargar DB)")
            self.clear_treeview()
            self.set_export_buttons_state("disabled")
            return

        conn = None
        try:
            conn = sqlite3.connect(self.current_db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            if not tables:
                self.table_selector.configure(values=["(No hay tablas)"])
                self.table_selector.set("(No hay tablas)")
                self.clear_treeview()
                self.set_export_buttons_state("disabled")
            else:
                self.table_selector.configure(values=tables)
                current_table_selection = self.table_selector.get()
                if current_table_selection in tables:
                    self.table_selector.set(current_table_selection)
                    self.display_table_content(current_table_selection)
                else:
                    self.table_selector.set(tables[0])
                    self.display_table_content(tables[0])
                self.set_export_buttons_state("normal")
        except sqlite3.Error as e:
            self.table_selector.configure(values=["(Error al cargar tablas)"])
            self.table_selector.set("(Error al cargar tablas)")
            self.clear_treeview()
            self.set_export_buttons_state("disabled")
            messagebox.showerror("Error de Base de Datos", f"Error al acceder a la base de datos '{db_name}': {e}")
        finally:
            if conn:
                conn.close()

    def clear_treeview(self):
        for item in self.db_treeview.get_children():
            self.db_treeview.delete(item)
        self.db_treeview["columns"] = ()
        self.db_treeview.heading("#0", text="")
        self.db_treeview.column("#0", width=0, stretch=False)

    def display_table_content(self, table_name):
        self.clear_treeview()

        if not self.current_db_path or not os.path.exists(self.current_db_path):
            messagebox.showwarning("Advertencia", "Por favor, seleccione una base de datos válida primero.")
            self.set_export_buttons_state("disabled")
            return
        
        if table_name in ["(Seleccione una DB primero)", "(No hay tablas)", "(Error al cargar tablas)"]:
            self.set_export_buttons_state("disabled")
            return

        conn = None
        try:
            conn = sqlite3.connect(self.current_db_path)
            df_table = pd.read_sql_query(f"SELECT * FROM \"{table_name}\"", conn)

            if df_table.empty:
                self.db_treeview.heading("#0", text="Tabla vacía")
                messagebox.showinfo("Tabla Vacía", f"La tabla '{table_name}' está vacía.")
                self.set_export_buttons_state("disabled")
            else:
                self.db_treeview["columns"] = list(df_table.columns)
                self.db_treeview.column("#0", width=0, stretch=False)

                for col in df_table.columns:
                    self.db_treeview.heading(col, text=col, anchor=ctk.W)
                    max_len_data = df_table[col].astype(str).apply(len).max() if not df_table.empty else 0
                    header_len = len(col)
                    width = max(max_len_data * 10, header_len * 10, 100)
                    self.db_treeview.column(col, width=width, minwidth=20, stretch=True, anchor=ctk.W)

                for index, row in df_table.iterrows():
                    values = [str(x) if pd.notna(x) else "" for x in row.values]
                    self.db_treeview.insert("", "end", values=values)
                
                self.db_treeview.grid(row=0, column=0, sticky="nsew", in_=self.tree_frame)
                self.set_export_buttons_state("normal")


        except Exception as e:
            self.clear_treeview()
            self.set_export_buttons_state("disabled")
            messagebox.showerror("Error de Visualización de Tabla", f"Error al mostrar el contenido de la tabla '{table_name}':\n{e}")
        finally:
            if conn:
                conn.close()

    def open_db_file_dialog(self):
        file_path = filedialog.askopenfilename(
            title="Seleccionar archivo de Base de Datos SQLite",
            filetypes=[("Archivos de Base de Datos SQLite", "*.db"), ("Todos los archivos", "*.*")]
        )
        if file_path:
            db_name = os.path.basename(file_path)
            current_values = list(self.db_selector.cget("values"))
            if db_name not in current_values:
                if current_values == ["(No se encontraron DBs)"] and db_name != "(No se encontraron DBs)":
                    self.db_selector.configure(values=[db_name])
                else:
                    current_values.append(db_name)
                    self.db_selector.configure(values=current_values)
            self.db_selector.set(db_name)
            self.current_db_path = file_path
            self.load_tables(db_name)

    # --- Funcionalidades de descarga general de archivos ---
    def populate_download_file_selector(self):
        """
        Llena el selector de archivos a descargar con archivos .db encontrados.
        """
        # Filtrar solo archivos .db
        all_files = [f for f in os.listdir('.') if f.endswith('.db')]
        
        if not all_files:
            self.file_download_selector.configure(values=["(No se encontraron archivos .db)"])
            self.file_download_selector.set("(No se encontraron archivos .db)")
            self.set_download_button_state("disabled")
            return
        
        self.file_download_selector.configure(values=all_files)
        self.file_download_selector.set(all_files[0])
        self.set_download_button_state("normal")

    def download_selected_file_threaded(self):
        """
        Inicia la descarga del archivo seleccionado en un hilo separado.
        """
        selected_file = self.file_download_selector.get()
        
        if selected_file == "(No se encontraron archivos .db)" or not os.path.exists(selected_file):
            messagebox.showwarning("Advertencia de Descarga", "Por favor, seleccione un archivo válido para descargar.")
            self.write("⚠️ Advertencia: No se puede descargar. Seleccione un archivo válido.\n")
            return

        self.write(f"\n--- INICIANDO DESCARGA DE ARCHIVO '{selected_file}' ---\n")
        self.set_download_button_state("disabled")

        download_thread = threading.Thread(target=self._execute_file_download_thread, args=(selected_file,))
        download_thread.start()

    def _execute_file_download_thread(self, source_file_path):
        """
        Lógica interna para ejecutar la descarga del archivo y manejar errores.
        Ofrece la opción de exportar tablas de DB a CSVs.
        """
        try:
            initial_file_name = os.path.basename(source_file_path)
            file_extension = os.path.splitext(initial_file_name)[1].lower()

            if file_extension == '.db':
                # Preguntar al usuario si desea exportar a CSV o descargar directamente el .db
                choice = messagebox.askyesno(
                    "Opciones de Descarga de Base de Datos",
                    f"El archivo '{initial_file_name}' es una base de datos SQLite.\n\n"
                    "¿Desea exportar todas sus tablas a archivos CSV separados?\n"
                    "(Si selecciona 'No', se descargará el archivo .db directamente)."
                )

                if choice: # El usuario eligió exportar a CSV(s)
                    self.write(f"ℹ️ Exportando tablas de '{initial_file_name}' a CSV(s)...\n")
                    # Diálogo para seleccionar la carpeta de destino para los CSVs
                    destination_directory = filedialog.askdirectory(
                        title=f"Seleccionar Carpeta para Guardar los Archivos CSV de '{initial_file_name}'"
                    )

                    if not destination_directory:
                        message = "ℹ️ Exportación de CSVs cancelada: No se seleccionó una carpeta de destino."
                        self.after(0, lambda: self.write(message + "\n"))
                        self.after(0, lambda: messagebox.showinfo("Exportación Cancelada", message))
                        return

                    conn = None
                    try:
                        conn = sqlite3.connect(source_file_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = [row[0] for row in cursor.fetchall()]

                        if not tables:
                            message = f"⚠️ La base de datos '{initial_file_name}' no contiene tablas para exportar."
                            self.after(0, lambda: self.write(message + "\n"))
                            self.after(0, lambda: messagebox.showwarning("Sin Tablas", message))
                            return

                        exported_files = []
                        for table_name in tables:
                            try:
                                df_table = pd.read_sql_query(f"SELECT * FROM \"{table_name}\"", conn)
                                # Asegurar que los nombres de los archivos CSV sean únicos y descriptivos
                                base_file_name = os.path.splitext(initial_file_name)[0]
                                output_csv_path = os.path.join(destination_directory, 
                                                               f"{base_file_name}_{table_name}.csv")
                                # CAMBIO CLAVE: Usar sep=';' para el delimitador de punto y coma
                                df_table.to_csv(output_csv_path, index=False, encoding='utf-8', sep=';')
                                exported_files.append(os.path.basename(output_csv_path))
                                self.write(f"✅ Tabla '{table_name}' exportada a '{os.path.basename(output_csv_path)}'.\n")
                            except Exception as table_e:
                                self.write(f"❌ Error al exportar tabla '{table_name}': {table_e}\n")
                        
                        if exported_files:
                            message = f"✅ Exportación de tablas completada. Archivos CSV guardados en '{destination_directory}'.\nArchivos: {', '.join(exported_files)}"
                            self.after(0, lambda: messagebox.showinfo("Exportación Completada", message))
                        else:
                            message = "⚠️ No se pudo exportar ninguna tabla a CSV. Verifique los logs para más detalles."
                            self.after(0, lambda: messagebox.showwarning("Exportación Incompleta", message))

                    except Exception as db_e:
                        message = f"❌ Error al acceder a la base de datos '{initial_file_name}' para exportar a CSV: {db_e}"
                        self.after(0, lambda: self.write(message + "\n"))
                        self.after(0, lambda: messagebox.showerror("Error de Base de Datos", message))
                    finally:
                        if conn:
                            conn.close()

                else: # El usuario eligió descargar el archivo .db directamente
                    save_path = filedialog.asksaveasfilename(
                        initialfile=initial_file_name,
                        defaultextension=file_extension,
                        filetypes=[(f"Archivos {file_extension.upper()}", f"*{file_extension}"), ("Todos los archivos", "*.*")]
                    )
                    if save_path:
                        shutil.copy(source_file_path, save_path)
                        message = f"✅ Archivo '{initial_file_name}' descargado exitosamente a:\n{save_path}"
                        self.after(0, lambda: self.write(message + "\n"))
                        self.after(0, lambda: messagebox.showinfo("Descarga Completada", message))
                    else:
                        message = "ℹ️ Descarga de archivo .db cancelada."
                        self.after(0, lambda: self.write(message + "\n"))
                        self.after(0, lambda: messagebox.showinfo("Descarga Cancelada", message))
            else: # No es un archivo .db (ej. .txt), proceder con descarga directa normal
                # Aunque ahora solo se listan .db, mantenemos esta rama por si en el futuro se vuelve a listar .txt u otros
                save_path = filedialog.asksaveasfilename(
                    initialfile=initial_file_name,
                    defaultextension=file_extension,
                    filetypes=[(f"Archivos {file_extension.upper()}", f"*{file_extension}"), ("Todos los archivos", "*.*")]
                )
                if save_path:
                    shutil.copy(source_file_path, save_path)
                    message = f"✅ Archivo '{initial_file_name}' descargado exitosamente a:\n{save_path}"
                    self.after(0, lambda: self.write(message + "\n"))
                    self.after(0, lambda: messagebox.showinfo("Descarga Completada", message))
                else:
                    message = "ℹ️ Descarga de archivo cancelada."
                    self.after(0, lambda: self.write(message + "\n"))
                    self.after(0, lambda: messagebox.showinfo("Descarga Cancelada", message))
        except Exception as e:
            error_message = f"❌ ERROR CRÍTICO DURANTE LA DESCARGA DEL ARCHIVO: {e}"
            self.after(0, lambda: self.write(error_message + "\n"))
            self.after(0, lambda: messagebox.showerror("Error de Descarga", error_message))
        finally:
            self.after(0, lambda: self.set_download_button_state("normal")) # Habilitar el botón de descarga

# Punto de entrada de la aplicación
if __name__ == "__main__":
    app = EtlApp()
    app.mainloop()
