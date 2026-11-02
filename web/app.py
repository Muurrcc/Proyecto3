import os
import threading
import time
from datetime import datetime, timezone

import psycopg
import requests
from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from psycopg.types.json import Jsonb

app = Flask(__name__)

CONEXION_POSTGRES = os.environ.get(
    "PG_DSN",
    "dbname=practica3 user=practica3 password=practica3 host=postgres port=5432",
)
URL_CONSULTAS_INFLUXDB = os.environ.get("INFLUX_URL", "http://influxdb:8086/query")
CREDENCIALES_INFLUXDB = (
    os.environ.get("INFLUX_USER", "root"),
    os.environ.get("INFLUX_PASS", "root"),
)
BASE_DE_DATOS_INFLUXDB = os.environ.get("INFLUX_DB", "renfe")
URL_PUBLICA_DE_GRAFANA = os.environ.get("GRAFANA_PUBLIC_URL", "http://localhost:3001")

SEGUNDOS_ENTRE_SINCRONIZACIONES = int(os.environ.get("SYNC_SECONDS", "30"))
SEGUNDOS_DE_SOLAPE_ENTRE_SINCRONIZACIONES = 120
HORAS_A_LEER_EN_LA_PRIMERA_SINCRONIZACION = 2
HORAS_DE_POSICIONES_GUARDADAS_EN_POSTGRES = 48

candado_de_sincronizacion = threading.Lock()


ESQUEMA_SQL_ADICIONAL = """
CREATE TABLE IF NOT EXISTS cancelaciones (
    tren_id     INTEGER NOT NULL REFERENCES trenes(id),
    trip        TEXT NOT NULL,
    origen      TEXT,
    destino     TEXT,
    origen_lat  DOUBLE PRECISION,
    origen_lon  DOUBLE PRECISION,
    primera_vez TIMESTAMPTZ NOT NULL,
    ultima_vez  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tren_id, trip)
);

CREATE OR REPLACE VIEW v_cancelados_ahora AS
SELECT c.primera_vez, c.ultima_vez, t.codigo AS tren, l.codigo AS linea, n.nombre AS nucleo, l.categoria,
       c.trip, c.origen, c.destino, c.origen_lat, c.origen_lon
FROM cancelaciones c
JOIN trenes t  ON t.id = c.tren_id
JOIN lineas l  ON l.id = t.linea_id
JOIN nucleos n ON n.id = l.nucleo_id
WHERE c.ultima_vez > now() - interval '15 minutes';

CREATE OR REPLACE FUNCTION trenes_en(ts timestamptz) RETURNS TABLE (
    observado_en timestamptz, tren text, linea text, nucleo text, lat double precision, lon double precision,
    vel_kmh real, retraso_s integer, estado text, parada text, en_movimiento boolean, categoria text)
LANGUAGE sql STABLE AS $$
    SELECT DISTINCT ON (p.tren_id)
           p.observado_en, t.codigo, l.codigo, n.nombre, p.lat, p.lon, p.vel_kmh, p.retraso_s, p.estado, pa.nombre,
           (p.estado <> 'STOPPED_AT'
            AND coalesce(abs(p.lat - r.lat) + abs(p.lon - r.lon) > 0.0003, true)),
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
    WHERE p.observado_en > ts - interval '15 minutes' AND p.observado_en <= ts
    ORDER BY p.tren_id, p.observado_en DESC
$$;
CREATE OR REPLACE VIEW v_trenes_ahora AS SELECT * FROM trenes_en(now());

CREATE TABLE IF NOT EXISTS incidentes (
    id          SERIAL PRIMARY KEY,
    titulo      TEXT NOT NULL,
    ocurrido_en TIMESTAMPTZ NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    aproximada  BOOLEAN NOT NULL DEFAULT true,
    trenes      JSONB NOT NULL DEFAULT '[]',
    detalle     TEXT NOT NULL DEFAULT '',
    fuentes     JSONB NOT NULL DEFAULT '[]',
    UNIQUE (titulo, ocurrido_en)
);
"""

INCIDENTES_ESCRITOS_A_MANO = [
    {
        "titulo": "Choque entre un AVE de Renfe y un Iryo cerca de Urda (Toledo)",
        "ocurrido_en": datetime(2026, 10, 28, 7, 53, tzinfo=timezone.utc),
        "lat": 39.485,
        "lon": -3.831,
        "aproximada": True,
        "trenes": [
            {
                "marca": "AVE",
                "operador": "Renfe",
                "numero": "02080",
                "trayecto": "Madrid - Sevilla",
                "viajeros": 397,
            },
            {
                "marca": "IRYO",
                "operador": "Iryo",
                "numero": "6076",
                "trayecto": "Madrid-Puerta de Atocha - Sevilla",
                "viajeros": 147,
            },
        ],
        "detalle": (
            "El AVE 2080 alcanzó por detrás al Iryo 6076, que iba por delante, entre Los Yébenes y Urda "
            "(hacia el PK 118 de la línea de alta velocidad Madrid-Andalucía). "
            "Adif suspendió la circulación en el corredor."
        ),
        "fuentes": [
            {
                "nombre": "El Español (directo)",
                "url": "https://www.elespanol.com/invertia/observatorios/movilidad/20261028/accidente-tren-hoy-directo-ultima-hora-choque-resultado-varios-heridos-interrumpido-circulacion-madrid-andalucia/1003744437595_10.html",
            },
            {
                "nombre": "El Debate",
                "url": "https://www.eldebate.com/economia/20261028/interrumpida-alta-velocidad-entre-madrid-andalucia-choque-alcance-entre-dos-trenes_474638.html",
            },
        ],
    }
]


def consultar_influxdb(consulta_influxql):
    respuesta = requests.get(
        URL_CONSULTAS_INFLUXDB,
        params={"db": BASE_DE_DATOS_INFLUXDB, "q": consulta_influxql, "epoch": "ms"},
        auth=CREDENCIALES_INFLUXDB,
        timeout=30,
    )
    respuesta.raise_for_status()

    resultado = respuesta.json()["results"][0]
    if "error" in resultado:
        raise RuntimeError(resultado["error"])

    return resultado.get("series", [])


def convertir_series_de_influxdb_a_diccionarios(series):
    for serie in series:
        nombres_de_columnas = serie["columns"]
        for valores in serie["values"]:
            yield dict(zip(nombres_de_columnas, valores))


def convertir_milisegundos_a_fecha(milisegundos):
    return datetime.fromtimestamp(milisegundos / 1000, timezone.utc)


def calcular_milisegundos_desde_los_que_leer(ultimo_instante_guardado):
    if ultimo_instante_guardado:
        segundos_desde = ultimo_instante_guardado.timestamp() - SEGUNDOS_DE_SOLAPE_ENTRE_SINCRONIZACIONES
    else:
        segundos_desde = time.time() - HORAS_A_LEER_EN_LA_PRIMERA_SINCRONIZACION * 3600

    return int(segundos_desde * 1000)


def construir_diccionario_de_claves_a_ids(cursor, consulta_sql):
    cursor.execute(consulta_sql)

    diccionario = {}
    for fila in cursor.fetchall():
        *campos_de_la_clave, id_en_postgres = fila
        clave = tuple(campos_de_la_clave) if len(campos_de_la_clave) > 1 else campos_de_la_clave[0]
        diccionario[clave] = id_en_postgres

    return diccionario


def cargar_ids_de_nucleos_lineas_y_trenes(cursor):
    ids_de_nucleos = construir_diccionario_de_claves_a_ids(cursor, "SELECT nombre, id FROM nucleos")
    ids_de_lineas = construir_diccionario_de_claves_a_ids(cursor, "SELECT nucleo_id, codigo, id FROM lineas")
    ids_de_trenes = construir_diccionario_de_claves_a_ids(cursor, "SELECT linea_id, codigo, id FROM trenes")

    return ids_de_nucleos, ids_de_lineas, ids_de_trenes


def obtener_o_crear_id_del_tren(cursor, ids_conocidos, punto_de_influxdb):
    ids_de_nucleos, ids_de_lineas, ids_de_trenes = ids_conocidos

    nombre_del_nucleo = punto_de_influxdb.get("nucleo") or "Desconocido"
    if nombre_del_nucleo not in ids_de_nucleos:
        cursor.execute("INSERT INTO nucleos (nombre) VALUES (%s) RETURNING id", (nombre_del_nucleo,))
        ids_de_nucleos[nombre_del_nucleo] = cursor.fetchone()[0]
    id_del_nucleo = ids_de_nucleos[nombre_del_nucleo]

    clave_de_la_linea = (id_del_nucleo, punto_de_influxdb.get("linea") or "?")
    if clave_de_la_linea not in ids_de_lineas:
        categoria_de_la_linea = punto_de_influxdb.get("categoria") or "Cercanias"
        cursor.execute(
            "INSERT INTO lineas (nucleo_id, codigo, categoria) VALUES (%s, %s, %s) RETURNING id",
            (id_del_nucleo, clave_de_la_linea[1], categoria_de_la_linea),
        )
        ids_de_lineas[clave_de_la_linea] = cursor.fetchone()[0]
    id_de_la_linea = ids_de_lineas[clave_de_la_linea]

    clave_del_tren = (id_de_la_linea, punto_de_influxdb["tren"])
    if clave_del_tren not in ids_de_trenes:
        cursor.execute(
            "INSERT INTO trenes (linea_id, codigo) VALUES (%s, %s) RETURNING id",
            clave_del_tren,
        )
        ids_de_trenes[clave_del_tren] = cursor.fetchone()[0]

    return ids_de_trenes[clave_del_tren]


def copiar_posiciones_de_influxdb_a_postgres(cursor):
    cursor.execute("SELECT max(observado_en) FROM posiciones")
    ultima_posicion_guardada = cursor.fetchone()[0]
    milisegundos_desde = calcular_milisegundos_desde_los_que_leer(ultima_posicion_guardada)

    consulta_influxql = (
        'SELECT "tren", "linea", "nucleo", "categoria", "parada_id", "parada", "estado", "lat", "lon", "vel", "retraso" '
        f'FROM "trenes" WHERE time > {milisegundos_desde}ms'
    )

    ids_conocidos = cargar_ids_de_nucleos_lineas_y_trenes(cursor)
    posiciones_a_guardar = []

    series = consultar_influxdb(consulta_influxql)
    for punto in convertir_series_de_influxdb_a_diccionarios(series):
        punto_sin_posicion_o_sin_tren = punto.get("lat") is None or punto.get("lon") is None or not punto.get("tren")
        if punto_sin_posicion_o_sin_tren:
            continue

        id_del_tren = obtener_o_crear_id_del_tren(cursor, ids_conocidos, punto)

        id_de_la_parada = punto.get("parada_id") or None
        if id_de_la_parada:
            cursor.execute(
                "INSERT INTO paradas (id, nombre) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                (id_de_la_parada, punto.get("parada") or id_de_la_parada),
            )

        retraso_en_segundos = None if punto.get("retraso") is None else int(punto["retraso"])

        posiciones_a_guardar.append((
            id_del_tren,
            convertir_milisegundos_a_fecha(punto["time"]),
            punto["lat"],
            punto["lon"],
            punto.get("vel"),
            retraso_en_segundos,
            punto.get("estado") or "",
            id_de_la_parada,
        ))

    if posiciones_a_guardar:
        cursor.executemany(
            """INSERT INTO posiciones (tren_id, observado_en, lat, lon, vel_kmh, retraso_s, estado, parada_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (tren_id, observado_en) DO NOTHING""",
            posiciones_a_guardar,
        )

    return len(posiciones_a_guardar)


def copiar_viajes_cancelados_de_influxdb_a_postgres(cursor):
    cursor.execute("SELECT max(ultima_vez) FROM cancelaciones")
    ultima_cancelacion_guardada = cursor.fetchone()[0]
    milisegundos_desde = calcular_milisegundos_desde_los_que_leer(ultima_cancelacion_guardada)

    consulta_influxql = (
        'SELECT "tren", "linea", "nucleo", "categoria", "trip", "origen", "destino", "origen_lat", "origen_lon" '
        f'FROM "trenes_cancelados" WHERE time > {milisegundos_desde}ms'
    )

    ids_conocidos = cargar_ids_de_nucleos_lineas_y_trenes(cursor)
    cancelaciones_por_tren_y_viaje = {}

    series = consultar_influxdb(consulta_influxql)
    for punto in convertir_series_de_influxdb_a_diccionarios(series):
        if not punto.get("tren") or not punto.get("trip"):
            continue

        momento_visto = convertir_milisegundos_a_fecha(punto["time"])
        id_del_tren = obtener_o_crear_id_del_tren(cursor, ids_conocidos, punto)

        cancelacion = cancelaciones_por_tren_y_viaje.setdefault(
            (id_del_tren, punto["trip"]),
            {
                "primera_vez": momento_visto,
                "ultima_vez": momento_visto,
                "origen": punto.get("origen"),
                "destino": punto.get("destino"),
                "origen_lat": punto.get("origen_lat"),
                "origen_lon": punto.get("origen_lon"),
            },
        )
        cancelacion["primera_vez"] = min(cancelacion["primera_vez"], momento_visto)
        cancelacion["ultima_vez"] = max(cancelacion["ultima_vez"], momento_visto)

    if cancelaciones_por_tren_y_viaje:
        filas_a_guardar = [
            (
                id_del_tren,
                viaje,
                cancelacion["origen"],
                cancelacion["destino"],
                cancelacion["origen_lat"],
                cancelacion["origen_lon"],
                cancelacion["primera_vez"],
                cancelacion["ultima_vez"],
            )
            for (id_del_tren, viaje), cancelacion in cancelaciones_por_tren_y_viaje.items()
        ]
        cursor.executemany(
            """INSERT INTO cancelaciones (tren_id, trip, origen, destino, origen_lat, origen_lon, primera_vez, ultima_vez)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (tren_id, trip) DO UPDATE SET
                   primera_vez = LEAST(cancelaciones.primera_vez, EXCLUDED.primera_vez),
                   ultima_vez = GREATEST(cancelaciones.ultima_vez, EXCLUDED.ultima_vez),
                   origen = EXCLUDED.origen, destino = EXCLUDED.destino,
                   origen_lat = EXCLUDED.origen_lat, origen_lon = EXCLUDED.origen_lon""",
            filas_a_guardar,
        )

    return len(cancelaciones_por_tren_y_viaje)


def crear_tablas_vistas_e_incidentes_que_faltan():
    with psycopg.connect(CONEXION_POSTGRES) as conexion:
        conexion.execute(ESQUEMA_SQL_ADICIONAL)

        for incidente in INCIDENTES_ESCRITOS_A_MANO:
            conexion.execute(
                """INSERT INTO incidentes (titulo, ocurrido_en, lat, lon, aproximada, trenes, detalle, fuentes)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (titulo, ocurrido_en) DO NOTHING""",
                (
                    incidente["titulo"],
                    incidente["ocurrido_en"],
                    incidente["lat"],
                    incidente["lon"],
                    incidente["aproximada"],
                    Jsonb(incidente["trenes"]),
                    incidente["detalle"],
                    Jsonb(incidente["fuentes"]),
                ),
            )


def calcular_retrasos_por_hora_y_por_dia_y_borrar_datos_antiguos(cursor):
    cursor.execute(
        """INSERT INTO retrasos_hora (hora, linea_id, posiciones, lecturas, retraso_medio, retraso_max, con_retraso)
           SELECT date_trunc('hour', p.observado_en), t.linea_id, count(*), count(p.retraso_s),
                  avg(p.retraso_s), max(p.retraso_s), count(*) FILTER (WHERE p.retraso_s >= 300)
           FROM posiciones p JOIN trenes t ON t.id = p.tren_id
           WHERE p.observado_en >= date_trunc('hour', now() - interval '2 hours')
           GROUP BY 1, 2 HAVING count(p.retraso_s) > 0
           ON CONFLICT (hora, linea_id) DO UPDATE SET posiciones = EXCLUDED.posiciones, lecturas = EXCLUDED.lecturas,
               retraso_medio = EXCLUDED.retraso_medio, retraso_max = EXCLUDED.retraso_max,
               con_retraso = EXCLUDED.con_retraso"""
    )

    cursor.execute(
        """INSERT INTO retrasos_dia (dia, nucleo_id, lecturas, retraso_medio, retraso_max, con_retraso)
           SELECT h.hora::date, l.nucleo_id, sum(h.lecturas), sum(h.retraso_medio * h.lecturas) / sum(h.lecturas),
                  max(h.retraso_max), sum(h.con_retraso)
           FROM retrasos_hora h JOIN lineas l ON l.id = h.linea_id
           WHERE h.hora >= date_trunc('day', now() - interval '1 day')
           GROUP BY 1, 2
           ON CONFLICT (dia, nucleo_id) DO UPDATE SET lecturas = EXCLUDED.lecturas,
               retraso_medio = EXCLUDED.retraso_medio, retraso_max = EXCLUDED.retraso_max,
               con_retraso = EXCLUDED.con_retraso"""
    )

    cursor.execute(
        "DELETE FROM posiciones WHERE observado_en < now() - make_interval(hours => %s)",
        (HORAS_DE_POSICIONES_GUARDADAS_EN_POSTGRES,),
    )
    cursor.execute(
        "DELETE FROM cancelaciones WHERE ultima_vez < now() - make_interval(hours => %s)",
        (HORAS_DE_POSICIONES_GUARDADAS_EN_POSTGRES,),
    )


def sincronizar_influxdb_con_postgres():
    with candado_de_sincronizacion:
        with psycopg.connect(CONEXION_POSTGRES) as conexion:
            try:
                with conexion.cursor() as cursor:
                    posiciones_copiadas = copiar_posiciones_de_influxdb_a_postgres(cursor)
                    copiar_viajes_cancelados_de_influxdb_a_postgres(cursor)
                    calcular_retrasos_por_hora_y_por_dia_y_borrar_datos_antiguos(cursor)
                    cursor.execute(
                        "INSERT INTO sincronizaciones (filas, estado) VALUES (%s, 'ok')",
                        (posiciones_copiadas,),
                    )
                return {"estado": "ok", "filas": posiciones_copiadas}

            except Exception as error:
                conexion.rollback()
                with conexion.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO sincronizaciones (estado, detalle) VALUES ('error', %s)",
                        (str(error)[:500],),
                    )
                return {"estado": "error", "detalle": str(error)}


def sincronizar_para_siempre_en_segundo_plano():
    tablas_ya_creadas = False

    while True:
        try:
            if not tablas_ya_creadas:
                crear_tablas_vistas_e_incidentes_que_faltan()
                tablas_ya_creadas = True

            print("sync:", sincronizar_influxdb_con_postgres(), flush=True)

        except Exception as error:
            print("sync fallo:", error, flush=True)

        time.sleep(SEGUNDOS_ENTRE_SINCRONIZACIONES)


def ejecutar_consulta_sql(consulta_sql, argumentos=()):
    with psycopg.connect(CONEXION_POSTGRES) as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(consulta_sql, argumentos)
            nombres_de_columnas = [columna.name for columna in cursor.description]
            return [dict(zip(nombres_de_columnas, fila)) for fila in cursor.fetchall()]


def calcular_resumen_en_instante(instante=None):
    return ejecutar_consulta_sql(
        """WITH v AS (SELECT * FROM trenes_en(%(t)s))
           SELECT (SELECT count(*) FROM v) AS trenes_ahora,
                  (SELECT count(*) FROM v WHERE en_movimiento) AS en_movimiento,
                  (SELECT count(*) FROM v WHERE NOT en_movimiento) AS parados,
                  (SELECT coalesce(round(avg(retraso_s)::numeric, 0), 0) FROM v) AS retraso_medio_s,
                  (SELECT count(*) FROM v WHERE retraso_s >= 300) AS con_retraso,
                  (SELECT count(*) FROM cancelaciones
                   WHERE primera_vez <= %(t)s AND ultima_vez > %(t)s - interval '15 minutes') AS cancelados,
                  (SELECT count(*) FROM nucleos) AS nucleos,
                  (SELECT count(*) FROM lineas) AS lineas,
                  (SELECT count(*) FROM posiciones) AS posiciones""",
        {"t": instante or datetime.now(timezone.utc)},
    )[0]


def leer_instante_pedido_en_la_url():
    texto_del_instante = request.args.get("t")
    if not texto_del_instante:
        return datetime.now(timezone.utc)

    try:
        instante = datetime.fromisoformat(texto_del_instante.replace("Z", "+00:00"))
    except ValueError:
        abort(400, "t debe ser una fecha ISO 8601")

    return instante if instante.tzinfo else instante.replace(tzinfo=timezone.utc)


def convertir_trenes_en_instante_a_geojson(instante):
    trenes = ejecutar_consulta_sql("SELECT * FROM trenes_en(%s)", (instante,))

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": f"{tren['nucleo']}-{tren['linea']}-{tren['tren']}",
                "geometry": {"type": "Point", "coordinates": [tren["lon"], tren["lat"]]},
                "properties": {
                    "tren": tren["tren"],
                    "linea": tren["linea"],
                    "nucleo": tren["nucleo"],
                    "vel": tren["vel_kmh"],
                    "retraso": tren["retraso_s"],
                    "estado": tren["estado"],
                    "parada": tren["parada"],
                    "movimiento": tren["en_movimiento"],
                    "categoria": tren["categoria"],
                    "hora": tren["observado_en"].isoformat(),
                },
            }
            for tren in trenes
        ],
    }


def convertir_cancelados_con_posicion_en_instante_a_geojson(instante):
    viajes_cancelados = ejecutar_consulta_sql(
        """SELECT c.primera_vez, t.codigo AS tren, l.codigo AS linea, n.nombre AS nucleo, l.categoria, c.trip,
                  c.origen, c.destino, c.origen_lat, c.origen_lon
           FROM cancelaciones c
           JOIN trenes t  ON t.id = c.tren_id
           JOIN lineas l  ON l.id = t.linea_id
           JOIN nucleos n ON n.id = l.nucleo_id
           WHERE c.origen_lat IS NOT NULL AND c.primera_vez <= %(t)s AND c.ultima_vez > %(t)s - interval '15 minutes'""",
        {"t": instante},
    )

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": f"{viaje['linea']}-{viaje['tren']}-{viaje['trip']}",
                "geometry": {"type": "Point", "coordinates": [viaje["origen_lon"], viaje["origen_lat"]]},
                "properties": {
                    "tren": viaje["tren"],
                    "linea": viaje["linea"],
                    "nucleo": viaje["nucleo"],
                    "categoria": viaje["categoria"],
                    "origen": viaje["origen"],
                    "destino": viaje["destino"],
                    "hora": viaje["primera_vez"].isoformat(),
                },
            }
            for viaje in viajes_cancelados
        ],
    }


@app.route("/")
def mostrar_pagina_principal():
    return render_template(
        "index.html",
        resumen=calcular_resumen_en_instante(),
        grafana=URL_PUBLICA_DE_GRAFANA,
        intervalo=SEGUNDOS_ENTRE_SINCRONIZACIONES,
    )


@app.route("/api/trenes")
def devolver_trenes_del_instante_en_geojson():
    return jsonify(convertir_trenes_en_instante_a_geojson(leer_instante_pedido_en_la_url()))


@app.route("/api/cancelados")
def devolver_cancelados_del_instante_en_geojson():
    return jsonify(convertir_cancelados_con_posicion_en_instante_a_geojson(leer_instante_pedido_en_la_url()))


@app.route("/api/instante")
def devolver_estado_del_mapa_en_el_instante():
    instante = leer_instante_pedido_en_la_url()

    return jsonify(
        t=instante.isoformat(),
        trenes=convertir_trenes_en_instante_a_geojson(instante),
        cancelados=convertir_cancelados_con_posicion_en_instante_a_geojson(instante),
        resumen=calcular_resumen_en_instante(instante),
    )


@app.route("/api/rango")
def devolver_cobertura_del_historico():
    posiciones_por_cubo_de_10_minutos = ejecutar_consulta_sql(
        """SELECT (floor(extract(epoch FROM observado_en) / 600) * 600000)::bigint AS t, count(*) AS n
           FROM posiciones GROUP BY 1 ORDER BY 1"""
    )
    primer_dato = ejecutar_consulta_sql(
        "SELECT (extract(epoch FROM min(observado_en)) * 1000)::bigint AS t FROM posiciones"
    )[0]["t"]

    return jsonify(
        desde=primer_dato,
        hasta=int(time.time() * 1000),
        cubo_ms=600000,
        cubos=posiciones_por_cubo_de_10_minutos,
    )


@app.route("/api/incidentes")
def devolver_incidentes_en_geojson():
    incidentes = ejecutar_consulta_sql("SELECT * FROM incidentes ORDER BY ocurrido_en")

    return jsonify(
        type="FeatureCollection",
        features=[
            {
                "type": "Feature",
                "id": incidente["id"],
                "geometry": {"type": "Point", "coordinates": [incidente["lon"], incidente["lat"]]},
                "properties": {
                    "titulo": incidente["titulo"],
                    "hora": incidente["ocurrido_en"].isoformat(),
                    "aproximada": incidente["aproximada"],
                    "trenes": incidente["trenes"],
                    "detalle": incidente["detalle"],
                    "fuentes": incidente["fuentes"],
                },
            }
            for incidente in incidentes
        ],
    )


@app.route("/api/resumen")
def devolver_resumen_del_instante():
    return jsonify(calcular_resumen_en_instante(leer_instante_pedido_en_la_url()))


@app.route("/sync", methods=["GET", "POST"])
def sincronizar_manualmente():
    resultado = sincronizar_influxdb_con_postgres()

    if resultado["estado"] != "ok":
        return jsonify(resultado), 502

    return redirect(url_for("mostrar_pagina_principal"))


@app.route("/health")
def comprobar_salud_del_servicio():
    ejecutar_consulta_sql("SELECT 1")
    return jsonify(estado="ok")


threading.Thread(target=sincronizar_para_siempre_en_segundo_plano, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
