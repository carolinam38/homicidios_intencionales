import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# Conectar a MySQL
DB_URL = "mysql+pymysql://root:@localhost/datamart"
engine = create_engine(DB_URL)

# Conectar a la base de datos staging (datalake)
datalake_engine = create_engine("mysql+pymysql://root:@localhost/datalake")
df = pd.read_sql("SELECT * FROM homicidios_limpios;", con=datalake_engine)

# ----------------------- 📌 Cargar Dimensión TIEMPO -----------------------
df_tiempo = df[["anio", "mes", "dia", "hora_infraccion"]].drop_duplicates()
df_tiempo.rename(columns={"hora_infraccion": "hora"}, inplace=True)

df_tiempo.to_sql(name="dim_tiempo", con=engine, if_exists="append", index=False, method="multi")

# ----------------------- 📌 Cargar Dimensión UBICACIÓN -----------------------
df_geo = df[["id_geo", "zona", "provincia", "canton", "tipo_lugar"]].drop_duplicates()

with engine.connect() as conn:
    for _, row in df_geo.iterrows():
        query = text("""
            INSERT IGNORE INTO dim_geo (id_geo, zona, provincia, canton, tipo_lugar)
            VALUES (:id_geo, :zona, :provincia, :canton, :tipo_lugar)
        """)
        conn.execute(query, row.to_dict())
    conn.commit()

# ----------------------- 📌 Función para Grupo Etario -----------------------
def asignar_grupo_etario_y_media(edad):
    """Convierte edad a número y la clasifica en grupos etarios con su edad media."""
    try:
        edad = int(edad)  # Convertir a entero
    except (ValueError, TypeError):
        return "Sin Dato", "Sin Dato"  # Manejo de valores inválidos

    if edad <= 11:
        return "0-11 años", 5
    elif edad <= 17:
        return "12-17 años", 14
    elif edad <= 25:
        return "18-25 años", 22
    elif edad <= 35:
        return "26-35 años", 30
    elif edad <= 45:
        return "36-45 años", 40
    elif edad <= 60:
        return "46-60 años", 53
    else:
        return "60+ años", 65

# ----------------------- 📌 Cargar Dimensión VÍCTIMA -----------------------
df_victima = df[["id_victima", "sexo", "edad", "estado_civil", "etnia", "discapacidad", "nacionalidad"]].drop_duplicates()

# Aplicar función de grupo etario
df_victima["grupo_etario"], df_victima["edad"] = zip(*df_victima["edad"].apply(lambda x: asignar_grupo_etario_y_media(x)))

# Reemplazar valores NaN con "Sin Dato"
df_victima = df_victima.fillna("Sin Dato")

with engine.connect() as conn:
    for _, row in df_victima.iterrows():
        query = text("""
            INSERT IGNORE INTO dim_victima (id_victima, sexo, edad, grupo_etario, estado_civil, etnia, discapacidad, nacionalidad)
            VALUES (:id_victima, :sexo, :edad, :grupo_etario, :estado_civil, :etnia, :discapacidad, :nacionalidad)
        """)
        conn.execute(query, row.to_dict())
    conn.commit()

# ----------------------- 📌 Cargar Dimensión DELITO -----------------------
df_delito = df[["id_delito", "tipo_muerte", "arma", "presunta_motivacion"]].drop_duplicates()
df_delito = df_delito.fillna("Sin Dato")  # Reemplazar valores vacíos

with engine.connect() as conn:
    for _, row in df_delito.iterrows():
        query = text("""
            INSERT IGNORE INTO dim_delito (id_delito, tipo_muerte, arma, presunta_motivacion)
            VALUES (:id_delito, :tipo_muerte, :arma, :presunta_motivacion)
        """)
        conn.execute(query, row.to_dict())
    conn.commit()

print("✅ Carga de dimensiones completada correctamente.")
