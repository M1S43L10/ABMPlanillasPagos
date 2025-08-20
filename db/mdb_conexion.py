import pypyodbc
from pathlib import Path



class MDBConnection:
    def __init__(self, path):
        self.mdb_path =  Path(path)
        self.connection = None
    
    def connect(self):
        try:
            self.connection = pypyodbc.connect(f'Driver={{Microsoft Access Driver (*.mdb)}};DBQ={self.mdb_path};')
            print("Conexión exitosa a la base de datos.")
            return True
        except pypyodbc.Error as e:
            print("Error al conectar a la base de datos:", e)
            return False

    
    def close(self):
        if self.connection:
            self.connection.close()
            print("Conexión cerrada.")
    
    def get_table_names(self):
        try:
            cursor = self.connection.cursor()
            cursor.tables()
            resultados = cursor.fetchall()
            table_names = [row[2] for row in resultados if row[3] == 'TABLE']
            cursor.close()
            return table_names
        except pypyodbc.Error as e:
            print("Error al obtener los nombres de las tablas:", e)
            return None

    def get_table_data(self, table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f'SELECT * FROM {table_name}')
            data = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            cursor.close()
            return data, column_names
        except pypyodbc.Error as e:
            print(f"Error al obtener los datos de la tabla {table_name}: {e}")
            return None, None
    
    def add_column(self, table_name, column_name, column_type):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}')
            self.connection.commit()
            cursor.close()
            print(f"Columna '{column_name}' añadida a la tabla '{table_name}'.")
        except pypyodbc.Error as e:
            print(f"Error al añadir la columna '{column_name}' a la tabla '{table_name}': {e}")

    def insert_data(self, table_name, column_name, value):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f'INSERT INTO {table_name} ({column_name}) VALUES (?)', [value])
            self.connection.commit()
            cursor.close()
            print(f"Dato '{value}' insertado en la columna '{column_name}' en la tabla '{table_name}'.")
        except pypyodbc.Error as e:
            print(f"Error al insertar el dato '{value}' en la columna '{column_name}' en la tabla '{table_name}': {e}")
    
    def get_column_data_types(self, table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f'SELECT * FROM {table_name}')
            column_types = {column[0]: column[1].__name__ for column in cursor.description}
            cursor.close()
            return column_types
        except pypyodbc.Error as e:
            print(f"Error al obtener los tipos de datos de la tabla {table_name}: {e}")
            return None
    
    def delete_row(self, table_name, condition):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f'DELETE FROM {table_name} WHERE {condition}')
            self.connection.commit()
            cursor.close()
            print(f"Fila eliminada de la tabla '{table_name}' donde {condition}.")
        except pypyodbc.Error as e:
            print(f"Error al eliminar la fila de la tabla '{table_name}': {e}")
    
    def actualizar_dato(self, tabla, columna, nuevo_valor, condicion):
        try:
            # Crear un cursor para ejecutar consultas SQL
            cursor = self.connection.cursor()
            
            # Construir la consulta SQL para actualizar los datos
            sql_query = f"UPDATE {tabla} SET {columna} = ? WHERE {condicion}"
            
            # Ejecutar la consulta SQL con el nuevo valor como parámetro
            cursor.execute(sql_query, (nuevo_valor,))
            
            # Confirmar los cambios en la base de datos
            self.connection.commit()
            
            # Cerrar el cursor y la conexión
            cursor.close()            
            print("Dato actualizado correctamente.")
            return True
        
        except Exception as e:
            print(f"Error: {str(e)}")
            return False
        
        
    def obtener_dato(self, tabla, columna):
        try:
            # Crear un cursor para ejecutar consultas SQL
            cursor = self.connection.cursor()
            
            # Construir la consulta SQL para obtener el dato
            sql_query = f"SELECT {columna} FROM {tabla}"
            
            # Ejecutar la consulta SQL
            cursor.execute(sql_query)
            
            # Obtener el dato
            dato = cursor.fetchone()[0]
            
            # Cerrar el cursor y la conexión
            cursor.close()
            
            return dato
        
        except Exception as e:
            print(f"Error: {str(e)}")
            return None
        
        
    def obtener_ISNULL(self, tabla, columna, condicion):
        try:
            # Crear un cursor para ejecutar consultas SQL
            cursor = self.connection.cursor()
            
            # Construir la consulta SQL para obtener el dato
            sql_query = f"SELECT {columna} FROM {tabla} WHERE {condicion}"
            
            # Ejecutar la consulta SQL
            cursor.execute(sql_query)
            
            # Obtener el dato
            dato = cursor.fetchone()[0]
            
            # Cerrar el cursor y la conexión
            cursor.close()
            
            return dato is None
        
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    
    def existe_columna(self, tabla, columna):
        try:
            cursor = self.connection.cursor()
            sql_query = (f"SELECT TOP 1 * FROM {tabla}")
            # Ejecutar la consulta SQL
            cursor.execute(sql_query)
            columnas = [columna[0] for columna in cursor.description]
            
            
            # Verificar si la columna está en la lista de nombres de columnas
            return columna in columnas
            
        except Exception as e:
            print(f"Error: {str(e)}")
            return False
        
    def existe_tabla(self, tabla):
        try:
            cursor = self.connection.cursor()
            # Consulta SQL para verificar si la tabla existe
            sql_query = f"SELECT TOP 1 * FROM {tabla}"
            # Ejecutar la consulta SQL
            cursor.execute(sql_query)
            # Si no hay errores, significa que la tabla existe
            return True
            
        except Exception as e:
            # Si hay un error, la tabla no existe o hay un problema de acceso
            print(f"Error: {str(e)}")
            return False


















































































































































"""
# Ejemplo de uso
mdb_connection = MDBConnection()
mdb_connection.connect()"""
"""
# Obtener los nombres de las tablas
table_names = mdb_connection.get_table_names()
if table_names:
    print("Tablas en la base de datos:")
    for table in table_names:
        print(table)

# Obtener datos de una tabla específica
table_name = 'EMPRESA'
column_names, table_data = mdb_connection.get_table_data(table_name)
if table_data is not None:
    print(f"Datos de la tabla '{table_name}':")
    print("Columnas:", column_names)
    print("Datos:")
    for row in table_data:
        print(row)

condicion = "'pathtxt = \\MOLINOS'"
dato_obtenido = mdb_connection.obtener_dato('EMPRESA', 'token_api_molinos', condicion)
print("Dato obtenido:", dato_obtenido)

# Cerrar la conexión
mdb_connection.close()
"""

"""
# Ejemplo de uso
dsn = 'MOLINOS'
mdb_connection = MDBConnection()
mdb_connection.connect()

# Agregar una columna a una tabla específica
table_name = 'EMPRESA'
column_name = 'cliente_ETL'
column_type = 'TEXT'
mdb_connection.add_column(table_name, column_name, column_type)

# Cerrar la conexión
mdb_connection.close()
"""



"""
# Ejemplo de uso
dsn = 'MOLINOS'
mdb_connection = MDBConnection()
mdb_connection.connect()

# Obtener los tipos de datos de una tabla específica
table_name = 'EMPRESA'
column_types = mdb_connection.get_column_types(table_name)
if column_types is not None:
    print(f"Tipos de datos de la tabla '{table_name}':")
    for column_type in column_types:
        print(column_type)

# Cerrar la conexión
mdb_connection.close()"""


"""
mdb_connection.delete_row('EMPRESA', "'cliente_ETL = molinos'")
"""


"""
# Ejemplo de uso
mdb_connection = MDBConnection()
mdb_connection.connect()

# Insertar un nuevo dato en la tabla
table_name = 'EMPRESA'
column_name = 'cliente_ETL'
new_name = 'molinos'
mdb_connection.insert_data(table_name, column_name, new_name)
# Cerrar la conexión
mdb_connection.close()"""