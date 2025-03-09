# Paso 1: Fuentes de datos

## Airbnb
https://insideairbnb.com/get-the-data/

## INE
Establecimientos, plazas, grados de ocupación y personal empleado por comunidades autónomas y provincias
https://www.ine.es/jaxiT3/Tabla.htm?t=2066&L=0

## Idealista
Informe de precios de venta en España de 2024
https://www.idealista.com/press-room/property-price-reports/sale/1/report/2024/

Informe de precios de alquiler en España de 2024
https://www.idealista.com/press-room/property-price-reports/rent/1/report/2024/

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
CREATE TABLE comunidades_autonomas (
    id SERIAL PRIMARY KEY,
    nombre TEXT UNIQUE NOT NULL
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

CREATE TABLE precios_vivienda (
    id SERIAL PRIMARY KEY,
    provincia_id INT REFERENCES provincias(id) ON DELETE CASCADE,
    fecha DATE NOT NULL,
    precio_m2 NUMERIC,
    variacion_mensual NUMERIC,
    variacion_trimestral NUMERIC,
    variacion_anual NUMERIC,
    es_alquiler BOOLEAN DEFAULT FALSE
);

CREATE TABLE ocupacion_hotelera (
    id SERIAL PRIMARY KEY,
    comunidad_autonoma_id INT REFERENCES comunidades_autonomas(id) ON DELETE CASCADE,
    establecimientos NUMERIC,
    plazas NUMERIC,
    personal_empleado NUMERIC,
    iph NUMERIC,
    periodo DATE
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
