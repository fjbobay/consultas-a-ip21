

## Crear una consulta a IP21 con python
****
El código que se ha proporcionado es un ejemplo de cómo obtener datos de la base de datos IP21 utilizando la librería **pyodbc** de **Python** y almacenarlos en un archivo csv utilizando la librería pandas. El algoritmo se divide en las siguientes etapas:

 1. Conexión a la base de datos: Se establece una conexión a la base de datos utilizando pyodbc y los parámetros de conexión específicos (driver, host y puerto).
 2. Consulta a la base de datos: Se construye una consulta SQL que especifica los tags a obtener, el rango de tiempo y los parámetros específicos de la consulta. Luego se ejecuta la consulta utilizando pd.read_sql() y se guarda el resultado en una variable.
 3. Almacenamiento de los datos: Se utiliza el método to_csv() de pandas para guardar los datos obtenidos en un archivo csv.
 4. Cierre de la conexión: Se cierra la conexión con la base de datos para evitar problemas de conexión.

En resumen, este algoritmo permite obtener datos de una base de datos específica, filtrarlos por tags y rango de tiempo y guardarlos en un archivo csv para su uso posterior.

---

 ### Ejemplo

 En este ejemplo, se crea una nueva instancia de la clase DataRetriever, se utiliza el método get_data para obtener los datos de los tags especificados en el rango de tiempo especificado, se utiliza el método save_to_csv para guardar los datos en un archivo csv y finalmente se utiliza el método close_connection para cerrar la conexión con la base de datos.

```python
from DataRetriever import DataRetriever

# Crear una nueva instancia de la clase
retriever = DataRetriever()

# Obtener datos de los tags 'TAG-01' y 'TAG-02' entre el 12 de enero de 2023 a las 00:00:00 y el 12 de enero de 2023 a las 23:59:59
tags = ('TAG-01', 'TAG-02')
start_time = '2023-01-12 00:00:00'
end_time = '2023-01-12 23:59:59'
data = retriever.get_data(tags, start_time, end_time)

# Guardar los datos en un archivo csv
retriever.save_to_csv(data, 'data.csv')

# Cerrar la conexión
retriever.close_connection()
```

---

### Requirimientos
La conexión a la base de datos IP21 se logra mediante el uso del driver AspenTech SQLplus, el cual es proporcionado por AspenTech y se adquiere a través de la instalación de Aspen SQLplus. Es importante asegurar que el driver esté disponible en el sistema antes de intentar conectarse a la base de datos.

Para validar los drivers disponibles en pyodbc, se puede utilizar el método drivers() de la siguiente manera:

```python
print(pyodbc.drivers())
```

```
output:
[
    'SQL Server',
    'PostgreSQL ANSI(x64)',
    'PostgreSQL Unicode(x64)',
    'ODBC Driver 17 for SQL Server',
    'HDBODBC',
    'ODBC Driver 18 for SQL Server',
    'CData ODBC Driver for SharePoint',
    'AspenTech ODBC driver for Production Record Manager',
 >> 'AspenTech SQLplus'
 ] 
```

En este caso, el output nos mostrará una lista con los drivers disponibles en el sistema, en la cual se incluirá "AspenTech SQLplus" si está disponible.
Es importante tener en cuenta que el driver debe estar instalado en el sistema y configurado correctamente para poder realizar la conexión con la base de datos.

---
 ### Descripción
La clase DataRetriever se encarga de facilitar la obtención de datos de una base de datos específica, filtrados por tags y rango de tiempo, y su almacenamiento en un archivo csv. Esta clase tiene las siguientes funciones:

 - **__init__** (self): 
  Este método es el constructor de la clase. En él se establece la conexión con la base de datos utilizando pyodbc y los parámetros de conexión específicos (driver, host y puerto).
    ```python
   def __init__(self):
        self.conn = pyodbc.connect("DRIVER={AspenTech SQLplus};HOST=<SERVER>;PORT=<PORT>")
    ```

- **get_data** (self, tags, start_time, end_time): 
  Este método permite obtener los datos de los tags especificados en el rango de tiempo especificado. Construye una consulta SQL que especifica los tags a obtener, el rango de tiempo y los parámetros específicos de la consulta. Luego ejecuta la consulta y devuelve el resultado.
    ```python
    def get_data(self, tags, start_time, end_time):
        sql = "SELECT NAME,TS,VALUE FROM HISTORY WHERE NAME IN ? AND PERIOD = 9000 AND REQUEST = 2 AND TS BETWEEN TIMESTAMP ? AND TIMESTAMP ?"
        result = pd.read_sql(sql, self.conn, params=(tags, start_time, end_time))
        result['date_time'] = pd.to_datetime(result['TS'],unit='s')
        return result
    ```

- **save_to_csv** (self, data, file_name): 
 Este método permite guardar los datos obtenidos en un archivo csv. Utiliza el método to_csv() de pandas para guardar los datos en el archivo especificado.
  ```python
   def save_to_csv(self, data, file_name):
        data.to_csv(file_name)
  ```

- **transform_data** (self, data): 
 Se encarga de convertir la tabla obtenida en el resultado de la consulta SQL en un formato más conveniente para el análisis de datos. En particular, esta función realiza las siguientes acciones:

  - Crea una nueva columna llamada "Fecha" que contiene la información de la columna "TS" en formato de fecha y hora.
  - Agrupa los datos por la columna "Fecha" y "NAME" y calcula el promedio de la columna "VALUE" para cada grupo.
  - Crea columnas separadas para cada "NAME" y su respectivo valor promedio.
  - Elimina las columnas originales "TS" y "NAME" y "VALUE"

  El resultado final es una tabla con una columna "Fecha" que contiene las fechas y horas de los datos, y varias columnas adicionales, cada una de las cuales contiene los valores promedio para un "NAME" específico. Esta tabla es más fácil de analizar y visualizar para el análisis de datos.
  ```python
    def transform_data(self, data):
        data['TS'] = pd.to_datetime(data['TS'])
        data = data.pivot(index='TS', columns='NAME', values='VALUE')
        return data
  ```

- **close_connection** (self)
   close_connection(self): Este método permite cerrar la conexión con la base de datos. Es importante llamar a este método después de haber obtenido los datos y guardarlos en un archivo para evitar problemas de conexión.
    ```python
    def close_connection(self):
        self.conn.close()
    ```
Al crear una instancia de esta clase se puede obtener los datos de una base de datos específica, filtrarlos por tags y rango de tiempo y guardarlos en un archivo csv para su uso posterior, sin tener que escribir cada uno de estos pasos cada vez.

---

#### Fuente y recursos:
[Pandas](https://pandas.pydata.org/)
[pyodbc](https://github.com/mkleehammer/pyodbc)
