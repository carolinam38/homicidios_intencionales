import pandas as pd
from sqlalchemy import create_engine

# Definir archivos de origen
file_2024 = "mdi_homicidiosintencionales_pm_2024_enero-noviembre.xlsx"
file_2014_2023 = "mdi_homicidiosintencionales_pm_2014-2023.xlsx"

# Cargar datos desde los archivos Excel
df_2024 = pd.read_excel(file_2024)
df_2014_2023 = pd.read_excel(file_2014_2023)

# Unir los DataFrames
df_combined = pd.concat([df_2014_2023, df_2024], ignore_index=True)

# Definir la conexión a la base de datos MariaDB
DB_URL = "mysql+pymysql://root:@localhost/datalake"
engine = create_engine(DB_URL)

# Definir nombre de la tabla
table_name = "homicidios"

# Cargar los datos en MySQL
df_combined.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

print("Datos cargados correctamente en la base de datos 'datalake', tabla 'homicidios'.")
