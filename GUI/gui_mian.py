from tkinter import messagebox
import ttkbootstrap as ttk
import os
from pprint import pprint
from Func.log_errorsV2 import log_error
from db.sybase_conexion import ConexionSybase
from db.DBFManager import DBFManager

class GUI_MAIN():
    def __init__(self, __version__):
        self.datos_dbf_local = None
        self.datos_dbf_server = None
        try:
            self.cargar_configuracion()
            self.conectar_dba()
        except Exception as e:
            print(e)
        
        
        
    def conectar_dba(self):
        try:
            if self.datos_dbf_local is not None and self.datos_dbf_server is not None:
                self.conexionDBA = ConexionSybase(**self.datos_dbf_local)
                self.conexionDBASERVER = ConexionSybase(**self.datos_dbf_server)
                if self.conexionDBA.conectar() and self.conexionDBASERVER.conectar():
                    print("Conectado")
                else:
                    log_error('No se puede lograr conexión con ninguna base de datos')
                    messagebox.showerror('Error con el DBA', 'No se puede lograr conexión con ninguna base de datos')
            else:
                log_error("El servidor no se encuentra en línea")
                messagebox.showerror("Error", "El servidor no se encuentra en línea")
        except Exception as e:
            log_error(f"Error en conectar_dba: {str(e)}")
    
    
    def cargar_configuracion(self):
        try:
            # Carga configuración local 
            self.datos_dbf_local = DBFManager(r"F:\Sp\FacturaP\Dbf\SYBASE.dbf").extraer_parametros_dns()[0]
            print(self.datos_dbf_local)
        except FileNotFoundError:
            log_error("Archivo de configuración local SYBASE.dbf no encontrado")
            return
        except Exception as e:
            log_error(f"Error al cargar configuración local: {str(e)}")
            return

        ruta_sybase6 = r"F:\Sp\FacturaP\Dbf\SYBASE6.dbf"
        ruta_sybase10 = r"F:\Sp\FacturaP\Dbf\SYBASE10.dbf"

        try:
            if os.path.exists(ruta_sybase6):
                self.datos_dbf_server = DBFManager(ruta_sybase6).extraer_parametros_dns()[0]
                print(self.datos_dbf_local, 6)
            elif os.path.exists(ruta_sybase10):
                self.datos_dbf_server = DBFManager(ruta_sybase10).extraer_parametros_dns()[0]
                print(self.datos_dbf_local, 10)
            else:
                log_error("Ninguno de los archivos SYBASE6.dbf ni SYBASE10.dbf fue encontrado")
        except Exception as e:
            log_error(f"Error al procesar archivo SYBASE.dbf: {str(e)}")