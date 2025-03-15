# Paso 1: Fuentes de datos

## Airbnb
https://insideairbnb.com/get-the-data/

## INE
Datos de ocupación hotelera: 

- https://www.ine.es/jaxiT3/Tabla.htm?t=25173
- https://www.ine.es/jaxiT3/Tabla.htm?t=2942
- https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736177015&menu=ultiDatos&idp=1254735576863
- Gasto de los turistas internacionales según motivo principal del viaje: https://www.ine.es/jaxiT3/Tabla.htm?t=23995
- Gasto de los turistas internacionales según país de residencia: https://www.ine.es/jaxiT3/Tabla.htm?t=10838
- Gasto de los turistas internacionales según comunidad autónoma de destino principal: https://www.ine.es/jaxiT3/Tabla.htm?t=10839

## Idealista
Informe de precios de venta en España 2006-2024
- https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/venta/historico/


Informe de precios de alquiler en España 2006-2024
- https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/alquiler/historico/


# Paso 2: Procesamiento de datos
Los datos se han descargado en formato CSV y se han procesado con Python y Pandas.

Para los datos de **airbnb**, la web **insideairbnb** nos permite descargas datos de diferentes ciudades.
En este caso, se han descargado los datos de apartamentos disponibles en:

- Barcenola
- Madrid
- Valencia
- Sevilla
- Málaga
- País Vasco
- Menorca
- Mallorca
- Girona

Una vez limpios, se han concatenado en un solo archivo CSV.

Para los datos de ocupacion hosterela del INE, he seleccionado para el año 2024 las columnas:

- Número de habitaciones estimadas
- Número de plazas estimadas
- Grado de ocupación por plazas
- Grado de ocupación por plazas en fin de semana
- Grado de ocupación por habitaciones
- Personal empleado

Los datos de **Idealista** se han copiado y pegado en un archivo txt y transformado a CSV.
Ejemplos de transformaciones:

- Transformar espacios en separadores de csv
- Eliminar caracteres especiales
- Formatear números: + 2.4 -> 2.4, - 2.4 -> -2.4, 5 m2 -> 5,...
- Quitar tildes

# Paso 3: Carga de datos
Se ha creado el modelo de datos en una base de datos Postgres

```sql
CREATE TABLE IF NOT EXISTS pais (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS comunidad_autonoma (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255)
);

CREATE TABLE provincias (
    id SERIAL PRIMARY KEY,
    nombre TEXT UNIQUE NOT NULL,
    comunidad_autonoma_id INT REFERENCES comunidades_autonomas(id) ON DELETE CASCADE
);

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

CREATE TABLE IF NOT EXISTS precios_vivienda (
    id SERIAL PRIMARY KEY,
    año INT,
    comunidad_autonoma_id INT REFERENCES comunidad_autonoma(id) ON DELETE CASCADE,
    precio_m2 NUMERIC,
    variacion_mensual NUMERIC,
    variacion_trimestral NUMERIC,
    variacion_anual NUMERIC,
    es_alquiler BOOLEAN
);

CREATE TABLE IF NOT EXISTS sector_hotelero (
    id SERIAL PRIMARY KEY,
    periodo DATE UNIQUE,
    pernoctaciones INT,
    estancia_media FLOAT,
    grado_ocupacion FLOAT,
    tarifa_media NUMERIC(10,2),
    indice_precios FLOAT
);

CREATE TABLE IF NOT EXISTS ocupacion_hotelera (
    id SERIAL PRIMARY KEY,
    comunidad_autonoma_id INT REFERENCES comunidad_autonoma(id) ON DELETE CASCADE,
    tipo_alojamiento TEXT,
    periodo DATE,
    establecimientos_abiertos INT,
    plazas INT,
    personal INT,
    iph FLOAT
);

CREATE TABLE IF NOT EXISTS gasto_turistas_pais (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    pais_id INT REFERENCES pais(id) ON DELETE CASCADE,
    gasto_medio_persona NUMERIC NOT NULL,
    gasto_medio_diario NUMERIC NOT NULL,
    duracion_media_viaje NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS gasto_turistas_destino (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    comunidad_autonoma_id INT REFERENCES comunidad_autonoma(id) ON DELETE CASCADE,
    gasto_medio_persona NUMERIC NOT NULL,
    gasto_medio_diario NUMERIC NOT NULL,
    duracion_media_viaje NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS motivo_viaje (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS gasto_turistas_motivo (
    id SERIAL PRIMARY KEY,
    periodo INT NULL,
    motivo_viaje_id INT REFERENCES motivo_viaje(id) ON DELETE CASCADE,
    gasto_medio_persona NUMERIC,
    gasto_medio_diario NUMERIC,
    duracion_media_viaje NUMERIC
);
```

# Consultas
Promedio de precio de Airbnb por tipo de alojamiento y provincia

```sql
SELECT 
    p.nombre AS provincia,
    a.room_type,
    ROUND(AVG(a.price), 2) AS precio_medio_airbnb
FROM airbnb a
JOIN provincias p ON a.provincia_id = p.id
GROUP BY p.nombre, a.room_type
ORDER BY p.nombre, precio_medio_airbnb DESC;
```

Relación entre Airbnb y el precio de la vivienda
```sql
SELECT 
    p.nombre AS provincia,
    COUNT(a.id) AS total_airbnb,
    ROUND(AVG(v.precio_m2), 2) AS precio_m2_vivienda
FROM airbnb a
JOIN provincias p ON a.provincia_id = p.id
JOIN precios_vivienda v ON v.provincia_id = p.id
WHERE v.es_alquiler = FALSE  -- Solo consideramos precio de venta
GROUP BY p.nombre
ORDER BY total_airbnb DESC;
```

Impacto de Airbnb en la ocupación hotelera
```sql
SELECT 
    p.nombre AS provincia,
    COUNT(a.id) AS total_airbnb,
    ROUND(AVG(o.total), 2) AS ocupacion_hotelera_media
FROM airbnb a
JOIN provincias p ON a.provincia_id = p.id
JOIN ocupacion_hotelera o ON o.provincia_id = p.id
WHERE o.indicador = 'Grado de ocupacion por plazas'
GROUP BY p.nombre
ORDER BY total_airbnb DESC;
```

# Tareas pendientes
- [ ] Crear tabla CCAA
- [ ] Cargar datos de CCAA
- [ ] Crear relaciones con tabla provincias
- [ ] Cargar datos de ocupación hotelera: https://www.ine.es/jaxiT3/Tabla.htm?t=25173, https://www.ine.es/jaxiT3/Tabla.htm?t=2942
- [ ] Crear tabla de precios de alquiler
- [ ] Cargar datos de precios de alquiler: https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/venta/historico/
- [ ] Crear tabla de precios de venta
- [ ] Cargar datos de precios de venta: https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/alquiler/historico/
