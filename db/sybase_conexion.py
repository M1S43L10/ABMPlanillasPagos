import pypyodbc
import traceback

class ConexionSybase:
    def __init__(self, **kwargs):
        self.conexion = None
        self.cursor = None
        self.usuario = kwargs["UID"]
        self.contrasena = kwargs["PWD"]
        self.dsn_name = kwargs["DSN"]

    #CONEXION DE LA BASE DE DATOS
    def conectar(self):
        try:
            self.conexion = pypyodbc.connect(
                DSN=self.dsn_name,
                user=self.usuario,
                password=self.contrasena,
                Driver="{Adaptive Server Anywhere 9.0}",                
            )
            self.cursor = self.conexion.cursor()  # Crea el cursor
            return True
        except pypyodbc.Error as err:
            print(f"Error al conectar a Sybase: {err}")
            return False
        
    def ejecutar_consulta(self, sentencia_sql):
        try:
            # Intentar conectar si no se ha hecho antes
            if not self.conexion:
                self.conectar()

            # Crear un cursor para ejecutar la sentencia SQL
            with self.conexion.cursor() as cursor:
                # Verificación antes de ejecutar la consulta para asegurarse de que la conexión esté activa
                try:
                    # Intentar ejecutar una consulta de prueba (como un SELECT vacío)
                    cursor.execute('SELECT 1')
                except pypyodbc.Error:
                    # Si la conexión se ha cerrado, reconectar
                    self.conectar()
                
                # Ahora ejecutar la consulta principal
                cursor.execute(sentencia_sql)
                
                # Obtener los resultados si la consulta es un SELECT
                if sentencia_sql.strip().upper().startswith('SELECT'):
                    resultados = cursor.fetchall()
                    return resultados
                else:
                    # Para otros tipos de consultas (INSERT, UPDATE, DELETE)
                    self.conexion.commit()
                    return "Operación exitosa"

        except pypyodbc.Error as e:
            error_traceback = traceback.format_exc()
            print(f"Error al recargar dispositivos: {e}\nTraceback:\n{error_traceback}")
            print(f"Error al ejecutar la consulta: {e}")
            return None
        except Exception as e:
            print(f"Error inesperado: {e}")
            return None
        
    def specify_search_condicion(self, nombre_tabla, nombre_columna, condicion, valor_condicion, valor_unico):
        try:
            self.conectar()
            with self.conexion.cursor() as cursor:
                # Consulta para obtener el valor de la columna 'id' por 'condicion'
                query = f"SELECT {nombre_columna} FROM {nombre_tabla} WHERE {condicion} = '{valor_condicion}'"
                print(query)
                cursor.execute(query)
                if valor_unico:
                    resultado = cursor.fetchall()
                else:
                    resultado = cursor.fetchone()

                if resultado is not None and valor_unico == False:
                    id_valor = resultado
                    return id_valor[0]
                elif resultado is not None and valor_unico == True:
                    id_valor = resultado
                    return id_valor
                else:
                    return None
        except pypyodbc.Error as err:
            print(f"Error al obtener el valor de 'id': {err}")
            return None
        
    def conectarServer(self):
        try:
            self.conexion = pypyodbc.connect(
                DSN=self.dsn_name,  # Nombre del DSN configurado en tu sistema
                user=self.usuario,
                password=self.contrasena,
                Driver="{Adaptive Server Anywhere 9.0}"                
            )
            self.cursor = self.conexion.cursor()  # Crea el cursor
            return True
        except pypyodbc.Error as err:
            print(f"Error al conectar a Sybase: {err}")
            return False

        
    #MANEJO A LA BASE DE DATOS
    def eliminar_base_de_datos(self, nombre_bd):
        try:
            with self.conexion.cursor() as cursor:
                consulta = f"DROP DATABASE {nombre_bd}"
                cursor.execute(consulta)
                print(f"Base de datos '{nombre_bd}' eliminada exitosamente.")
        except pypyodbc.Error as err:
            print(f"Error al eliminar la base de datos: {err}")
            
            
    def desconectar(self):
        try:
            if self.conexion and self.conexion.connected:
                self.conexion.close()
            else:
                print("La conexión ya estaba cerrada.")
        except pypyodbc.Error as err:
            print(f"Error al cerrar la conexión: {err}")
            
            