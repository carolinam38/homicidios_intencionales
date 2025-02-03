import pandas as pd
from sqlalchemy import create_engine, text

# Conectar a MySQL
DB_URL = "mysql+pymysql://root:@localhost/datamart"
engine = create_engine(DB_URL)

# Conectar a la base de datos staging (datalake)
datalake_engine = create_engine("mysql+pymysql://root:@localhost/datalake")
df = pd.read_sql("SELECT * FROM homicidios_limpios;", con=datalake_engine)

# Verificar dimensiones cargadas
df_tiempo = pd.read_sql("SELECT * FROM dim_tiempo;", con=engine)

# Verificar que `dim_tiempo` tiene `id_tiempo`
if "id_tiempo" not in df_tiempo.columns:
    print("Error: La tabla `dim_tiempo` no tiene la columna `id_tiempo`")
    exit(1)

# Crear `fact_df` asegurando que usamos `hora_infraccion`
fact_df = df[["id_geo", "id_victima", "id_delito", "anio", "mes", "dia", "hora_infraccion", "sexo"]].copy()

# Reemplazar valores no numéricos en `hora_infraccion` por un valor predeterminado (ej. 0)
fact_df["hora"] = fact_df["hora_infraccion"].str.extract(r'(\d+)')  # Extraer solo los números
fact_df["hora"] = fact_df["hora"].fillna(0).astype(int)  # Reemplazar NaN por 0 y convertir a entero

# Hacer el merge con `dim_tiempo`
fact_df = fact_df.merge(df_tiempo, on=["anio", "mes", "dia", "hora"], how="left")

# Verificar valores faltantes en `id_tiempo`
missing_id_tiempo = fact_df["id_tiempo"].isna().sum()
if missing_id_tiempo > 0:
    print(f"⚠️ Advertencia: {missing_id_tiempo} filas no tienen `id_tiempo` asociado.")

# Eliminar registros con `NaN` en `id_tiempo` para evitar error de clave foránea
fact_df = fact_df.dropna(subset=["id_tiempo"])

# Convertir `id_tiempo` a entero (MySQL lo requiere como `INT`)
fact_df["id_tiempo"] = fact_df["id_tiempo"].astype(int)

# Agregar métricas
fact_df["cantidad_victimas"] = 1
fact_df["cantidad_hombres"] = (fact_df["sexo"] == "hombre").astype(int)
fact_df["cantidad_mujeres"] = (fact_df["sexo"] == "mujer").astype(int)

# Seleccionar solo las columnas necesarias
fact_df = fact_df[["id_geo", "id_victima", "id_delito", "id_tiempo", "cantidad_victimas", "cantidad_hombres", "cantidad_mujeres"]]

# Insertar datos en lotes pequeños (batch insert)
batch_size = 1000
with engine.connect() as conn:
    try:
        for i in range(0, len(fact_df), batch_size):
            batch = fact_df.iloc[i:i+batch_size].to_dict(orient="records")
            query = text("""
                INSERT INTO hechos_homicidios (id_geo, id_victima, id_delito, id_tiempo, cantidad_victimas, cantidad_hombres, cantidad_mujeres)
                VALUES (:id_geo, :id_victima, :id_delito, :id_tiempo, :cantidad_victimas, :cantidad_hombres, :cantidad_mujeres)
            """)
            conn.execute(query, batch)
        conn.commit()
        print("✅ Carga de hechos completada correctamente.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en la carga de hechos: {e}")
