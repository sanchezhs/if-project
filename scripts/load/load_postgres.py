import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
from logging import getLogger

logger = getLogger(__name__)
logger.setLevel("INFO")
logger.addHandler(logging.StreamHandler())

formatter = logging.Formatter("[%(levelname)s]: %(asctime)s - %(message)s")
logger.handlers[0].setFormatter(formatter)


load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# List of provinces with their corresponding comunidad autónoma
provincias = [
    ("Barcelona", "Cataluña"),
    ("Madrid", "Madrid, Comunidad de"),
    ("Malaga", "Andalucia"),
    ("Sevilla", "Andalucia"),
    ("Valencia", "Comunitat Valenciana"),
    ("Girona", "Cataluña"),
    ("Mallorca", "Balears, Illes"),
    ("Menorca", "Balears, Illes"),
    ("Euskadi", "Pais Vasco"),
]

# List of comunidades autónomas
ccaa = [
    "Andalucia",
    "Aragon",
    "Asturias, Principado de",
    "Balears, Illes",
    "Canarias",
    "Cantabria",
    "Castilla y Leon",
    "Castilla - La Mancha",
    "Cataluña",
    "Comunitat Valenciana",
    "Extremadura",
    "Galicia",
    "Madrid, Comunidad de",
    "Murcia, Region de",
    "Navarra, Comunidad Foral de",
    "Pais Vasco",
    "Rioja, La",
    "Ceuta",
    "Melilla",
]

# Lista paises
paises = ["Alemania", "Francia", "Italia", "Paises Nordicos", "Reino Unido", "Otros"]

motivos = [
    "Negocio, motivos profesionales",
    "Asistencia a ferias, congresos y convenciones",
    "Trabajador estacional temporero",
    "Otros motivos de trabajo y negocio",
    "Estudios (educacion y formacion)",
    "Visitas a familiares o amigos",
    "Tratamiento de salud voluntario",
    "Motivos religiosos o peregrinaciones",
    "Compras, servicios personales",
    "Ocio, recreo y vacaciones",
    "Turismo cultural",
    "Practica deportiva",
    "Turismo de sol y playa",
    "Otro tipo de ocio",
    "Otros",
]

def load_paises(engine):
    with engine.begin() as connection:
        for pais in paises:
            connection.execute(
                text(
                    "INSERT INTO pais (nombre) VALUES (:nombre) ON CONFLICT DO NOTHING"
                ),
                {"nombre": pais},
            )
    paises_dict = pd.read_sql("SELECT id, nombre FROM pais", engine)
    paises_dict = dict(zip(paises_dict["nombre"], paises_dict["id"]))

    return paises_dict

def load_motivos(engine):
    with engine.begin() as connection:
        for motivo in motivos:
            connection.execute(
                text(
                    "INSERT INTO motivo_viaje (nombre) VALUES (:nombre) ON CONFLICT DO NOTHING"
                ),
                {"nombre": motivo},
            )
    motivos_mapping = pd.read_sql("SELECT id, nombre FROM motivo_viaje", engine)
    motivos_dict = dict(zip(motivos_mapping["nombre"], motivos_mapping["id"]))

    return motivos_dict

def create_ccaa_provinces(engine):
    with engine.begin() as connection:
        for comunidad in ccaa:
            connection.execute(
                text(
                    "INSERT INTO comunidad_autonoma (nombre) VALUES (:nombre) ON CONFLICT DO NOTHING"
                ),
                {"nombre": comunidad},
            )

    # Retrieve the IDs for the comunidades autónomas
    ccaa_mapping = pd.read_sql("SELECT id, nombre FROM comunidad_autonoma", engine)
    ccaa_dict = dict(zip(ccaa_mapping["nombre"], ccaa_mapping["id"]))

    # Insert provinces with their corresponding comunidad autónoma ID
    with engine.begin() as connection:
        for provincia in provincias:
            province_name, comunidad = provincia
            comunidad_id = ccaa_dict.get(comunidad)
            connection.execute(
                text("""
                    INSERT INTO provincias (nombre, comunidad_autonoma_id)
                    VALUES (:nombre, :ccaa_id)
                    ON CONFLICT DO NOTHING
                """),
                {"nombre": province_name, "ccaa_id": comunidad_id},
            )

    # Retrieve the IDs for the provinces
    provincia_mapping = pd.read_sql("SELECT id, nombre FROM provincias", engine)
    provincia_dict = dict(zip(provincia_mapping["nombre"], provincia_mapping["id"]))

    return ccaa_dict, provincia_dict


def drop_tables(engine):
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS airbnb"))
        connection.execute(text("DROP TABLE IF EXISTS precios_vivienda"))
        connection.execute(text("DROP TABLE IF EXISTS ocupacion_hotelera"))
        connection.execute(text("DROP TABLE IF EXISTS provincias"))
        connection.execute(text("DROP TABLE IF EXISTS sector_hotelero"))
        connection.execute(text("DROP TABLE IF EXISTS gasto_turistas_pais"))
        connection.execute(text("DROP TABLE IF EXISTS gasto_turistas_destino"))
        connection.execute(text("DROP TABLE IF EXISTS gasto_turistas_motivo"))
        connection.execute(text("DROP TABLE IF EXISTS motivo_viaje"))
        connection.execute(text("DROP TABLE IF EXISTS comunidad_autonoma"))
        connection.execute(text("DROP TABLE IF EXISTS pais"))


def create_tables(engine):
    with engine.begin() as connection:
        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS pais (
                    id SERIAL PRIMARY KEY,
                    nombre VARCHAR(255) UNIQUE NOT NULL
                )
            """)
        )
        connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS comunidad_autonoma (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(255)
            )
            """)
        )
        connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS provincias (
                id SERIAL PRIMARY KEY,
                nombre TEXT UNIQUE NOT NULL,
                comunidad_autonoma_id INT REFERENCES comunidad_autonoma(id) ON DELETE CASCADE
            )
            """)
        )
        connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS airbnb (
                id SERIAL PRIMARY KEY,
                listing_id BIGINT UNIQUE NOT NULL,
                name TEXT,
                host_id BIGINT,
                host_name TEXT,
                neighbourhood_group TEXT,
                neighbourhood TEXT,
                latitude FLOAT,
                longitude FLOAT,
                room_type TEXT,
                price NUMERIC,
                minimum_nights INT,
                number_of_reviews INT,
                last_review DATE,
                reviews_per_month NUMERIC,
                calculated_host_listings_count INT,
                availability_365 INT,
                number_of_reviews_ltm INT,
                license TEXT,
                provincia_id INT REFERENCES provincias(id) ON DELETE CASCADE
            )
            """)
        )
        connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS precios_vivienda (
                id SERIAL PRIMARY KEY,
                año INT,
                comunidad_autonoma_id INT REFERENCES comunidad_autonoma(id) ON DELETE CASCADE,
                precio_m2 NUMERIC,
                variacion_mensual NUMERIC,
                variacion_trimestral NUMERIC,
                variacion_anual NUMERIC,
                es_alquiler BOOLEAN
            )
            """)
        )
        connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS ocupacion_hotelera (
                id SERIAL PRIMARY KEY,
                comunidad_autonoma_id INT REFERENCES comunidad_autonoma(id) ON DELETE CASCADE,
                tipo_alojamiento TEXT,
                periodo DATE,
                establecimientos_abiertos INT,
                plazas INT,
                personal INT,
                iph FLOAT
            )
            """)
        )
        connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS sector_hotelero (
                id SERIAL PRIMARY KEY,
                periodo DATE UNIQUE,
                pernoctaciones INT,
                estancia_media FLOAT,
                grado_ocupacion FLOAT,
                tarifa_media NUMERIC(10,2),
                indice_precios FLOAT
            )
            """)
        )
        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS gasto_turistas_pais (
                    id SERIAL PRIMARY KEY,
                    periodo DATE NOT NULL,
                    pais_id INT REFERENCES pais(id) ON DELETE CASCADE,
                    gasto_medio_persona NUMERIC NOT NULL,
                    gasto_medio_diario NUMERIC NOT NULL,
                    duracion_media_viaje NUMERIC NOT NULL
                )
            """)
        )
        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS gasto_turistas_destino (
                    id SERIAL PRIMARY KEY,
                    periodo DATE NOT NULL,
                    comunidad_autonoma_id INT REFERENCES comunidad_autonoma(id) ON DELETE CASCADE,
                    gasto_medio_persona NUMERIC NOT NULL,
                    gasto_medio_diario NUMERIC NOT NULL,
                    duracion_media_viaje NUMERIC NOT NULL
                )
            """)
        )
        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS motivo_viaje (
                    id SERIAL PRIMARY KEY,
                    nombre VARCHAR(255) UNIQUE NOT NULL
                )
            """)
        )
        connection.execute(
            text("""
            CREATE TABLE IF NOT EXISTS gasto_turistas_motivo (
                id SERIAL PRIMARY KEY,
                periodo INT NOT NULL,
                motivo_viaje_id INT REFERENCES motivo_viaje(id) ON DELETE CASCADE,
                gasto_medio_persona NUMERIC,
                gasto_medio_diario NUMERIC,
                duracion_media_viaje NUMERIC
            )
            """)
        )


def load_csv_to_db(file_path, table_name, has_province=False, rental=None):
    df = pd.read_csv(file_path, sep=",", encoding="utf-8")

    if has_province:
        # Map province names to their IDs
        df["provincia_id"] = df["provincia"].map(provincia_dict)
        df.drop(columns=["provincia"], inplace=True)

    if rental is not None:
        df["es_alquiler"] = rental

    df.to_sql(table_name, engine, if_exists="append", index=False)


if __name__ == "__main__":
    logger.info("Borrando tablas en PostgreSQL")
    drop_tables(engine)

    logger.info("Creando tablas en PostgreSQL")
    create_tables(engine)

    logger.info("Cargando datos de paises")
    paises_dict = load_paises(engine)

    logger.info("Cargando tabla provincias y comunidades autónomas")
    ccaa_dict, provincia_dict = create_ccaa_provinces(engine)

    logger.info("Cargando tabla motivos de viaje")
    motivos_dict = load_motivos(engine)

    # Load Airbnb data and map the province to its ID
    logger.info("Cargando datos de airbnb")
    load_csv_to_db(os.path.join("data", "airbnb", "airbnb.csv"), "airbnb", has_province=True)

    # Load hotel occupancy data and map the comunidad autónoma to its ID
    logger.info("Cargando datos de ocupación hotelera")
    ocupacion_hotelera = os.path.join("data", "ocupacion_hotelera", "merged.csv")
    df = pd.read_csv(ocupacion_hotelera, sep=",", encoding="utf-8")
    df["comunidad_autonoma_id"] = df["comunidad_autonoma"].map(ccaa_dict)
    df.drop(columns=["comunidad_autonoma"], inplace=True)
    df.to_sql("ocupacion_hotelera", engine, if_exists="append", index=False)

    # Load housing prices data and map the comunidad autónoma to its ID
    logger.info("Cargando datos de precios de vivienda")
    precios_vivienda = os.path.join("data", "precios_vivienda", "precios_vivienda.csv")
    df = pd.read_csv(precios_vivienda, sep=",", encoding="utf-8")
    df["comunidad_autonoma_id"] = df["comunidad_autonoma"].map(ccaa_dict)
    df.drop(columns=["comunidad_autonoma"], inplace=True)
    df.to_sql("precios_vivienda", engine, if_exists="append", index=False)

    logger.info("Cargando datos de turismo")
    sector_hotelero = os.path.join("data", "sector_hotelero", "sector_hotelero.csv")
    df = pd.read_csv(sector_hotelero, sep=",", encoding="utf-8")
    df.to_sql("sector_hotelero", engine, if_exists="append", index=False)

    logger.info("Cargando datos de gasto turistas por motivo de viaje")
    df = pd.read_csv(os.path.join("data", "gasto_motivo_viaje", "gasto_motivo_viaje_cleaned.csv"))
    df["motivo_viaje_id"] = df["motivo_viaje"].map(motivos_dict)
    df.drop(columns=["motivo_viaje"], inplace=True)
    df.to_sql("gasto_turistas_motivo", engine, if_exists="append", index=False)

    logger.info("Cargando datos de gasto turistas por destino y país")
    gasto_ccaa_destino = (
        os.path.join("data", "gasto_ccaa_destino", "gasto_ccaa_destino_cleaned.csv")
    )
    df = pd.read_csv(gasto_ccaa_destino, sep=",", encoding="utf-8")
    df["comunidad_autonoma_id"] = df["comunidad_autonoma"].map(ccaa_dict)
    df.drop(columns=["comunidad_autonoma"], inplace=True)
    df.to_sql("gasto_turistas_destino", engine, if_exists="append", index=False)

    logger.info("Cargando datos de gasto turistas por país de residencia")
    gasto_pais_residencia = os.path.join("data", "gasto_pais_residencia", "gasto_pais_residencia_cleaned.csv")
    df = pd.read_csv(gasto_pais_residencia, sep=",", encoding="utf-8")
    df["pais_id"] = df["pais_residencia"].map(paises_dict)
    df.drop(columns=["pais_residencia"], inplace=True)
    df.to_sql("gasto_turistas_pais", engine, if_exists="append", index=False)

    logger.info("Datos cargados en PostgreSQL")
