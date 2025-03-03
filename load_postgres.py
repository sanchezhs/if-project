import pandas as pd
from sqlalchemy import create_engine

# Configuración de conexión a PostgreSQL
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

# Crear conexión con PostgreSQL
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Cargar provincias desde los datasets de Airbnb
provincias = ["Barcelona", "Madrid", "Malaga", "Sevilla", "Valencia", "Girona", "Mallorca", "Menorca", "Euskadi"]
df_provincias = pd.DataFrame({"nombre": provincias})
df_provincias.to_sql("provincias", engine, if_exists="append", index=False)

provincia_mapping = pd.read_sql("SELECT id, nombre FROM provincias", engine)
provincia_dict = dict(zip(provincia_mapping["nombre"], provincia_mapping["id"]))

# Función para cargar datos CSV en PostgreSQL
def load_csv_to_db(file_path, table_name, has_province=False, rental=None):
    df = pd.read_csv(file_path, sep=",", encoding="utf-8")
    
    if has_province:
        df["provincia_id"] = df["provincia"].map(provincia_dict)
        df.drop(columns=["provincia"], inplace=True)

    if rental is not None:
        # Añadir columna de tipo de vivienda
        df["es_alquiler"] = rental

    df.to_sql(table_name, engine, if_exists="append", index=False)

# Cargar datos de Airbnb
load_csv_to_db("data/airbnb.csv", "airbnb", has_province=True)

# # Cargar datos de precios de vivienda
load_csv_to_db("data/precio_venta_2024.csv", "precios_vivienda", has_province=True, rental=False)
load_csv_to_db("data/precio_alquiler_2024.csv", "precios_vivienda", has_province=True, rental=True)

# Cargar datos de ocupación hotelera
load_csv_to_db("data/ocupacion_hostelera_2024_clean.csv", "ocupacion_hotelera", has_province=True)

print("✅ Datos cargados exitosamente en PostgreSQL")