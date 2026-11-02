-- Base de datos de la practica 3: trenes de Cercanias (Renfe), agregados y
-- relacionados con los datos que Telegraf guarda en InfluxDB.
-- Se ejecuta una sola vez, al crear el volumen de Postgres.

-- Dimension: nucleos de Cercanias (Madrid, Sevilla, Rodalies...)
CREATE TABLE nucleos (
    id      SERIAL PRIMARY KEY,
    nombre  TEXT NOT NULL UNIQUE
);

-- Dimension: lineas de cada nucleo (C1 de Madrid y C1 de Valencia son lineas distintas).
-- En el nucleo "Larga y Media Distancia" la "linea" es el tipo de servicio (AVE, AVLO, ALVIA, MD...).
CREATE TABLE lineas (
    id         SERIAL PRIMARY KEY,
    nucleo_id  INTEGER NOT NULL REFERENCES nucleos(id),
    codigo     TEXT NOT NULL,
    categoria  TEXT NOT NULL DEFAULT 'Cercanias',   -- Cercanias / Media distancia / Larga distancia / Alta velocidad
    UNIQUE (nucleo_id, codigo)
);

-- Dimension: paradas (nombre sacado del GTFS estatico de Renfe)
CREATE TABLE paradas (
    id      TEXT PRIMARY KEY,   -- stop_id de Renfe
    nombre  TEXT NOT NULL
);

-- Dimension: trenes (circulacion con su numero, dentro de una linea)
CREATE TABLE trenes (
    id        SERIAL PRIMARY KEY,
    linea_id  INTEGER NOT NULL REFERENCES lineas(id),
    codigo    TEXT NOT NULL,    -- numero de tren (ej. 23835)
    UNIQUE (linea_id, codigo)
);

-- Hechos: cada posicion recibida (Influx -> Postgres). Se conservan 48 h.
CREATE TABLE posiciones (
    id            BIGSERIAL PRIMARY KEY,
    tren_id       INTEGER NOT NULL REFERENCES trenes(id),
    observado_en  TIMESTAMPTZ NOT NULL,
    lat           DOUBLE PRECISION NOT NULL,
    lon           DOUBLE PRECISION NOT NULL,
    vel_kmh       REAL,                                 -- estimada entre dos posiciones
    retraso_s     INTEGER,                              -- de trip_updates; NULL si Renfe no lo da
    estado        TEXT NOT NULL DEFAULT '',             -- IN_TRANSIT_TO, INCOMING_AT, STOPPED_AT
    parada_id     TEXT REFERENCES paradas(id),          -- siguiente parada / parada actual
    UNIQUE (tren_id, observado_en)
);
CREATE INDEX ON posiciones (observado_en);

-- Agregado por hora y linea (calculado en SQL a partir de posiciones)
CREATE TABLE retrasos_hora (
    hora           TIMESTAMPTZ NOT NULL,
    linea_id       INTEGER NOT NULL REFERENCES lineas(id),
    posiciones     INTEGER NOT NULL,
    lecturas       INTEGER NOT NULL,           -- posiciones con retraso conocido
    retraso_medio  REAL NOT NULL,              -- segundos
    retraso_max    INTEGER NOT NULL,
    con_retraso    INTEGER NOT NULL,           -- lecturas con 5 min o mas de retraso
    PRIMARY KEY (hora, linea_id)
);

-- Agregado por dia y nucleo, calculado desde retrasos_hora
CREATE TABLE retrasos_dia (
    dia            DATE NOT NULL,
    nucleo_id      INTEGER NOT NULL REFERENCES nucleos(id),
    lecturas       INTEGER NOT NULL,
    retraso_medio  REAL NOT NULL,
    retraso_max    INTEGER NOT NULL,
    con_retraso    INTEGER NOT NULL,
    PRIMARY KEY (dia, nucleo_id)
);

-- Registro de cada sincronizacion Influx -> Postgres hecha por Flask
CREATE TABLE sincronizaciones (
    id        SERIAL PRIMARY KEY,
    inicio    TIMESTAMPTZ NOT NULL DEFAULT now(),
    filas     INTEGER NOT NULL DEFAULT 0,
    estado    TEXT NOT NULL,
    detalle   TEXT
);

-- Vista: ultima posicion de cada tren (los que han dado señal en los ultimos 15 min)
CREATE VIEW v_trenes_ahora AS
SELECT DISTINCT ON (p.tren_id)
       p.observado_en, t.codigo AS tren, l.codigo AS linea, n.nombre AS nucleo,
       p.lat, p.lon, p.vel_kmh, p.retraso_s, p.estado, pa.nombre AS parada,
       -- en movimiento: Renfe no lo marca STOPPED_AT y su posicion ha cambiado (> ~30 m) en los ultimos 3 min.
       -- Sin posicion de hace 3 min (tren recien visto) se decide solo por el estado.
       (p.estado <> 'STOPPED_AT'
        AND coalesce(abs(p.lat - r.lat) + abs(p.lon - r.lon) > 0.0003, true)) AS en_movimiento,
       l.categoria
FROM posiciones p
JOIN trenes t  ON t.id = p.tren_id
JOIN lineas l  ON l.id = t.linea_id
JOIN nucleos n ON n.id = l.nucleo_id
LEFT JOIN paradas pa ON pa.id = p.parada_id
LEFT JOIN LATERAL (
    SELECT q.lat, q.lon FROM posiciones q
    WHERE q.tren_id = p.tren_id AND q.observado_en <= p.observado_en - interval '3 minutes'
    ORDER BY q.observado_en DESC LIMIT 1
) r ON true
WHERE p.observado_en > now() - interval '15 minutes'
ORDER BY p.tren_id, p.observado_en DESC;

-- Vista: resumen por linea (ultima hora)
CREATE VIEW v_lineas AS
SELECT n.nombre AS nucleo, l.codigo AS linea,
       count(DISTINCT p.tren_id) AS trenes,
       round(avg(p.retraso_s)::numeric, 0) AS retraso_medio_s,
       max(p.retraso_s) AS retraso_max_s,
       round(100.0 * count(*) FILTER (WHERE p.retraso_s < 300) / nullif(count(p.retraso_s), 0), 1) AS pct_puntual
FROM posiciones p
JOIN trenes t  ON t.id = p.tren_id
JOIN lineas l  ON l.id = t.linea_id
JOIN nucleos n ON n.id = l.nucleo_id
WHERE p.observado_en > now() - interval '1 hour'
GROUP BY n.nombre, l.codigo;

-- Vista: resumen por nucleo (ultima hora)
CREATE VIEW v_nucleos AS
SELECT n.nombre AS nucleo,
       count(DISTINCT p.tren_id) AS trenes,
       round(avg(p.retraso_s)::numeric, 0) AS retraso_medio_s,
       max(p.retraso_s) AS retraso_max_s,
       round(100.0 * count(*) FILTER (WHERE p.retraso_s < 300) / nullif(count(p.retraso_s), 0), 1) AS pct_puntual
FROM posiciones p
JOIN trenes t  ON t.id = p.tren_id
JOIN lineas l  ON l.id = t.linea_id
JOIN nucleos n ON n.id = l.nucleo_id
WHERE p.observado_en > now() - interval '1 hour'
GROUP BY n.nombre;
