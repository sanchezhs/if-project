-- Crear la base de datos (ya se crea con docker-compose, pero aseguramos el contexto)
CREATE DATABASE airbnb_db;

-- Conectar a la base de datos
\c airbnb_db;

-- Crear tabla de provincias
CREATE TABLE provincias (
    id SERIAL PRIMARY KEY,
    nombre TEXT UNIQUE NOT NULL
);

-- Crear tabla de Airbnb
CREATE TABLE airbnb (
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
);


-- Crear tabla de precios de vivienda
CREATE TABLE precios_vivienda (
    id SERIAL PRIMARY KEY,
    provincia_id INT REFERENCES provincias(id) ON DELETE CASCADE,
    precio_m2 NUMERIC,
    variacion_mensual NUMERIC,
    variacion_trimestral NUMERIC,
    variacion_anual NUMERIC,
    maximo_historico NUMERIC,
    variacion_maximo NUMERIC,
    es_alquiler BOOLEAN,
);

-- Crear tabla de ocupación hotelera
CREATE TABLE ocupacion_hotelera (
    id SERIAL PRIMARY KEY,
    provincia_id INT REFERENCES provincias(id) ON DELETE CASCADE,
    indicador TEXT,
    periodo DATE,
    total NUMERIC
);