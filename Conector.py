import pandas as pd
import pyodbc

# Clase para obtener datos de una base de datos y guardarlos en un archivo csv.
class DataRetriever:
    # Inicializa una conexión con la base de datos.
    def __init__(self):
        self.conn = pyodbc.connect("DRIVER={AspenTech SQLplus};HOST=<SERVER>;PORT=<PORT>")

    def get_data(self, tags, start_time, end_time):
        """
        Obtiene los datos de la base de datos para los tags especificados en el rango de tiempo especificado.

        Args:
        - tags (list): Lista de tags para los cuales se quieren obtener los datos.
        - start_time (str): Fecha y hora de inicio en formato 'YYYY-MM-DD HH:MM:SS'.
        - end_time (str): Fecha y hora de fin en formato 'YYYY-MM-DD HH:MM:SS'.

        Returns:
        - pandas DataFrame con los datos obtenidos.
        """
        sql = "SELECT NAME,TS,VALUE FROM HISTORY WHERE NAME IN ? AND PERIOD = 9000 AND REQUEST = 2 AND TS BETWEEN TIMESTAMP ? AND TIMESTAMP ?"
        result = pd.read_sql(sql, self.conn, params=(tags, start_time, end_time))
        result['date_time'] = pd.to_datetime(result['TS'],unit='s')
        return result
    # Se encarga de convertir la tabla obtenida en el resultado de la consulta SQL en un formato más conveniente para el análisis de datos
    def transform_data(self, data):
        data['TS'] = pd.to_datetime(data['TS'])
        data = data.pivot(index='TS', columns='NAME', values='VALUE')
        return data

    # Guarda los datos especificados en un archivo csv.
    def save_to_csv(self, data, file_name):
        data.to_csv(file_name)

    # Cierra la conexión con la base de datos.
    def close_connection(self):
        self.conn.close()


