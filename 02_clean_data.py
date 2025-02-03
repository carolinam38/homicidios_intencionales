import pandas as pd
from sqlalchemy import create_engine

# Conectar a la base de datos
DB_URL = "mysql+pymysql://root:@localhost/datalake"
engine = create_engine(DB_URL)

# Leer datos desde la base de datos
query = "SELECT * FROM homicidios;"
df = pd.read_sql(query, con=engine)

# Verificar las columnas disponibles
print("Columnas en el DataFrame:", df.columns.tolist())

# Verificar si la columna 'fecha_infraccion' existe antes de proceder
if "fecha_infraccion" in df.columns:
    # Eliminar filas con valores nulos en columnas clave
    df.dropna(subset=["provincia", "canton", "sexo", "tipo_muerte", "fecha_infraccion"], inplace=True)
    
    # Convertir la columna de fecha_infraccion en formato datetime
    df["fecha_infraccion"] = pd.to_datetime(df["fecha_infraccion"], errors='coerce')
    
    # Extraer componentes de fecha_infraccion y hora
    df["anio"] = df["fecha_infraccion"].dt.year
    df["mes"] = df["fecha_infraccion"].dt.month
    df["dia"] = df["fecha_infraccion"].dt.day
    df["hora"] = df["fecha_infraccion"].dt.hour
else:
    print("Advertencia: La columna 'fecha_infraccion' no está presente en los datos.")

# Convertir nombres de texto a minúsculas y eliminar espacios en blanco
df["provincia"] = df["provincia"].str.strip().str.lower()
df["canton"] = df["canton"].str.strip().str.lower()
df["zona"] = df["zona"].str.strip().str.lower()
df["sexo"] = df["sexo"].str.strip().str.lower()
df["tipo_muerte"] = df["tipo_muerte"].str.strip().str.lower()
df["arma"] = df["arma"].str.strip().str.lower()

# Eliminar duplicados
df.drop_duplicates(inplace=True)

# Crear nuevas columnas derivadas (ejemplo: grupo etario)
df["grupo_etario"] = df["medida_edad"].map({"A": "adulto", "M": "menor"})

# Crear claves foráneas para dimensiones
df["id_geo"] = df["zona"] + "-" + df["provincia"] + "-" + df["canton"]
df["id_victima"] = df["sexo"] + "-" + df["grupo_etario"] + "-" + df["estado_civil"]
df["id_delito"] = df["tipo_muerte"] + "-" + df["arma"] + "-" + df["presunta_motivacion"]

# Guardar los datos limpios en una nueva tabla
df.to_sql(name="homicidios_limpios", con=engine, if_exists="replace", index=False)

print("Limpieza y transformación completadas. Datos guardados en 'homicidios_limpios'.")
