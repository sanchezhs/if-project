import pandas as pd
from sqlalchemy import create_engine

DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

provincias = [
    "Barcelona",
    "Madrid",
    "Malaga",
    "Sevilla",
    "Valencia",
    "Girona",
    "Mallorca",
    "Menorca",
    "Euskadi",
]
ccaa = [
    "Andalucía",
    "Aragón",
    "Asturias, Principado de",
    "Balears, Illes",
    "Canarias",
    "Cantabria",
    "Castilla y León",
    "Castilla - La Mancha",
    "Cataluña",
    "Comunitat Valenciana",
    "Extremadura",
    "Galicia",
    "Madrid, Comunidad de",
    "Murcia, Región de",
    "Navarra, Comunidad Foral de",
    "País Vasco",
    "Rioja, La",
    "Ceuta ",
    "Melilla",
]
df_provincias = pd.DataFrame({"nombre": provincias})
df_provincias.to_sql("provincias", engine, if_exists="append", index=False)

df_ccaa = pd.DataFrame({"nombre": ccaa})
df_ccaa.to_sql("ccaa", engine, if_exists="append", index=False)

provincia_mapping = pd.read_sql("SELECT id, nombre FROM provincias", engine)
provincia_dict = dict(zip(provincia_mapping["nombre"], provincia_mapping["id"]))

def drop_tables(engine):
    with engine.connect() as connection:
        connection.execute("DROP TABLE IF EXISTS airbnb")
        connection.execute("DROP TABLE IF EXISTS precios_vivienda")
        connection.execute("DROP TABLE IF EXISTS ocupacion_hotelera")
        connection.execute("DROP TABLE IF EXISTS provincias")
        connection.execute("DROP TABLE IF EXISTS ccaa")

def load_csv_to_db(file_path, table_name, has_province=False, rental=None):
    df = pd.read_csv(file_path, sep=",", encoding="utf-8")

    if has_province:
        df["provincia_id"] = df["provincia"].map(provincia_dict)
        df.drop(columns=["provincia"], inplace=True)

    if rental is not None:
        df["es_alquiler"] = rental

    df.to_sql(table_name, engine, if_exists="append", index=False)

if __name__ == "__main__":
    drop_tables(engine)

    load_csv_to_db("data/airbnb.csv", "airbnb", has_province=True)
    load_csv_to_db(
        "data/precio_venta_2024.csv", "precios_vivienda", has_province=True, rental=False
    )
    load_csv_to_db(
        "data/precio_alquiler_2024.csv", "precios_vivienda", has_province=True, rental=True
    )
    load_csv_to_db(
        "data/ocupacion_hostelera_2024_clean.csv", "ocupacion_hotelera", has_province=True
    )

    print("Datos cargados en PostgreSQL")