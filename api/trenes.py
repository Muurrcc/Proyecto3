import csv
import io
import json
import math
import os
import re
import socket
import time
import urllib.request
import zipfile
from collections import defaultdict, namedtuple

URL_BASE_FEEDS_RENFE_TIEMPO_REAL = os.environ.get("RENFE_RT", "https://gtfsrt.renfe.com")
URL_GTFS_CERCANIAS = "https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip"
URL_GTFS_LARGA_DISTANCIA = "https://ssl.renfe.com/gtransit/Fichero_AV_LD/google_transit.zip"

CARPETA_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
RUTA_CACHE_GTFS_CERCANIAS = os.path.join(CARPETA_CACHE, "fomento_transit.zip")
RUTA_CACHE_GTFS_LARGA_DISTANCIA = os.path.join(CARPETA_CACHE, "ld_transit.zip")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TOPIC_BASE_MQTT = "renfe"

SEGUNDOS_ENTRE_CONSULTAS_A_RENFE = 30
SEGUNDOS_SIN_POSICION_NUEVA_PARA_DAR_UN_TREN_DE_CERCANIAS_POR_INACTIVO = 600
SEGUNDOS_DE_RETRASO_PARA_CONTAR_COMO_RETRASADO = 300
SEGUNDOS_PARA_REPUBLICAR_UN_TREN_DE_LARGA_DISTANCIA_QUIETO = 120
SEGUNDOS_PARA_REPUBLICAR_UN_VIAJE_CANCELADO = 600
SEGUNDOS_PARA_OLVIDAR_UN_TREN_NO_VISTO = 3600
SEGUNDOS_DE_ANTIGUEDAD_MAXIMA_DEL_GTFS_DESCARGADO = 7 * 86400

NOMBRE_DEL_NUCLEO_DE_LARGA_DISTANCIA = "Larga y Media Distancia"

NUCLEO_POR_PREFIJO_DE_RUTA_DE_CERCANIAS = {
    "10": "Madrid",
    "20": "Asturias",
    "30": "Sevilla",
    "31": "Cadiz",
    "32": "Malaga",
    "40": "Valencia",
    "41": "Alicante",
    "45": "Murcia/Cartagena",
    "46": "Ferrol",
    "47": "Leon",
    "51": "Barcelona (Rodalies)",
    "60": "Bilbao",
    "61": "San Sebastian",
    "62": "Cantabria",
    "70": "Zaragoza",
}

CATEGORIA_POR_TIPO_DE_SERVICIO_DE_LARGA_DISTANCIA = {
    "AVE": "Alta velocidad",
    "AVE INT": "Alta velocidad",
    "AVLO": "Alta velocidad",
    "AVANT": "Alta velocidad",
    "AVANT EXP": "Alta velocidad",
    "MD": "Media distancia",
    "REGIONAL": "Media distancia",
    "REG.EXP.": "Media distancia",
    "PROXIMDAD": "Media distancia",
}
CATEGORIA_SI_EL_TIPO_DE_SERVICIO_NO_ESTA_EN_LA_TABLA = "Larga distancia"

VELOCIDAD_MAXIMA_CREIBLE_EN_KMH_POR_CATEGORIA = {"Alta velocidad": 330, "Larga distancia": 220}
VELOCIDAD_MAXIMA_CREIBLE_EN_KMH_SI_LA_CATEGORIA_NO_ESTA_EN_LA_TABLA = 160

DatosGtfsEstaticos = namedtuple(
    "DatosGtfsEstaticos",
    [
        "nucleo_por_viaje_de_cercanias",
        "tipo_de_servicio_por_numero_de_tren_de_larga_distancia",
        "nombre_por_id_de_parada",
        "primera_y_ultima_parada_por_numero_de_tren_de_larga_distancia",
        "coordenadas_por_id_de_parada_de_larga_distancia",
    ],
)

UltimaPosicionPublicada = namedtuple(
    "UltimaPosicionPublicada",
    ["timestamp_de_la_posicion", "latitud", "longitud", "instante_en_que_se_publico"],
)


def descargar_url(url, como_bytes=False):
    peticion = urllib.request.Request(url, headers={"User-Agent": "practica3-renfe/1.0"})
    with urllib.request.urlopen(peticion, timeout=30) as respuesta:
        contenido = respuesta.read()

    return contenido if como_bytes else json.loads(contenido)


def leer_filas_de_un_csv_dentro_del_zip(fichero_zip, nombre_del_csv):
    with fichero_zip.open(nombre_del_csv) as fichero_csv:
        for fila in csv.DictReader(io.TextIOWrapper(fichero_csv, encoding="utf-8-sig")):
            yield {columna.strip(): (valor or "").strip() for columna, valor in fila.items() if columna}


def abrir_gtfs_descargandolo_si_esta_viejo_o_no_existe(url, ruta_de_la_copia_local):
    copia_local_vieja_o_inexistente = (
        not os.path.exists(ruta_de_la_copia_local)
        or time.time() - os.path.getmtime(ruta_de_la_copia_local) > SEGUNDOS_DE_ANTIGUEDAD_MAXIMA_DEL_GTFS_DESCARGADO
    )

    if copia_local_vieja_o_inexistente:
        print(f"Descargando {url} ...", flush=True)
        os.makedirs(os.path.dirname(ruta_de_la_copia_local), exist_ok=True)
        with open(ruta_de_la_copia_local, "wb") as fichero:
            fichero.write(descargar_url(url, como_bytes=True))

    return zipfile.ZipFile(ruta_de_la_copia_local)


def calcular_primera_y_ultima_parada_de_cada_viaje(fichero_zip):
    extremos_por_viaje = {}

    for fila in leer_filas_de_un_csv_dentro_del_zip(fichero_zip, "stop_times.txt"):
        secuencia = int(fila["stop_sequence"])
        parada = fila["stop_id"]

        primera_y_ultima = extremos_por_viaje.setdefault(fila["trip_id"], [(secuencia, parada), (secuencia, parada)])
        if secuencia < primera_y_ultima[0][0]:
            primera_y_ultima[0] = (secuencia, parada)
        if secuencia > primera_y_ultima[1][0]:
            primera_y_ultima[1] = (secuencia, parada)

    return extremos_por_viaje


def cargar_gtfs_estaticos_de_cercanias_y_larga_distancia():
    nucleo_por_viaje = {}
    tipo_de_servicio_por_numero_de_tren = {}
    nombre_por_id_de_parada = {}
    coordenadas_por_id_de_parada = {}
    primera_y_ultima_parada_por_numero_de_tren = {}

    try:
        with abrir_gtfs_descargandolo_si_esta_viejo_o_no_existe(URL_GTFS_CERCANIAS, RUTA_CACHE_GTFS_CERCANIAS) as gtfs:
            nucleo_por_viaje = {
                fila["trip_id"]: fila["route_id"][:2]
                for fila in leer_filas_de_un_csv_dentro_del_zip(gtfs, "trips.txt")
            }
            nombre_por_id_de_parada.update({
                fila["stop_id"]: fila["stop_name"]
                for fila in leer_filas_de_un_csv_dentro_del_zip(gtfs, "stops.txt")
            })
    except Exception as error:
        print(f"Aviso: sin GTFS de Cercanias ({error}); los nucleos saldran como 'Desconocido'", flush=True)

    try:
        with abrir_gtfs_descargandolo_si_esta_viejo_o_no_existe(
            URL_GTFS_LARGA_DISTANCIA, RUTA_CACHE_GTFS_LARGA_DISTANCIA
        ) as gtfs:
            tipo_de_servicio_por_id_de_ruta = {
                fila["route_id"]: fila["route_short_name"]
                for fila in leer_filas_de_un_csv_dentro_del_zip(gtfs, "routes.txt")
            }

            viajes = list(leer_filas_de_un_csv_dentro_del_zip(gtfs, "trips.txt"))
            tipo_de_servicio_por_numero_de_tren = {
                viaje["trip_short_name"]: tipo_de_servicio_por_id_de_ruta.get(viaje["route_id"], "")
                for viaje in viajes
            }
            numero_de_tren_por_id_de_viaje = {viaje["trip_id"]: viaje["trip_short_name"] for viaje in viajes}

            paradas = list(leer_filas_de_un_csv_dentro_del_zip(gtfs, "stops.txt"))
            nombre_por_id_de_parada.update({parada["stop_id"]: parada["stop_name"] for parada in paradas})
            coordenadas_por_id_de_parada.update({
                parada["stop_id"]: (float(parada["stop_lat"]), float(parada["stop_lon"]))
                for parada in paradas
                if parada.get("stop_lat") and parada.get("stop_lon")
            })

            extremos_por_id_de_viaje = calcular_primera_y_ultima_parada_de_cada_viaje(gtfs)
            primera_y_ultima_parada_por_numero_de_tren = {
                numero_de_tren_por_id_de_viaje[id_de_viaje]: (extremos[0][1], extremos[1][1])
                for id_de_viaje, extremos in extremos_por_id_de_viaje.items()
                if id_de_viaje in numero_de_tren_por_id_de_viaje
            }
    except Exception as error:
        print(f"Aviso: sin GTFS de Larga Distancia ({error}); no se mostraran esos trenes", flush=True)

    print(
        f"GTFS estatico: {len(nucleo_por_viaje)} viajes de Cercanias, "
        f"{len(tipo_de_servicio_por_numero_de_tren)} de LD, {len(nombre_por_id_de_parada)} paradas",
        flush=True,
    )

    return DatosGtfsEstaticos(
        nucleo_por_viaje,
        tipo_de_servicio_por_numero_de_tren,
        nombre_por_id_de_parada,
        primera_y_ultima_parada_por_numero_de_tren,
        coordenadas_por_id_de_parada,
    )


def calcular_distancia_en_km_entre_dos_coordenadas(latitud_1, longitud_1, latitud_2, longitud_2):
    radianes_por_grado = math.pi / 180

    haversine = (
        math.sin((latitud_2 - latitud_1) * radianes_por_grado / 2) ** 2
        + math.cos(latitud_1 * radianes_por_grado)
        * math.cos(latitud_2 * radianes_por_grado)
        * math.sin((longitud_2 - longitud_1) * radianes_por_grado / 2) ** 2
    )

    diametro_de_la_tierra_en_km = 12742
    return diametro_de_la_tierra_en_km * math.asin(math.sqrt(haversine))


class ClienteMqttMinimo:
    def __init__(self, identificador_del_cliente="renfe_trenes"):
        self.identificador_del_cliente = identificador_del_cliente
        self.conexion = None

    @staticmethod
    def codificar_texto_con_su_longitud(texto_en_bytes):
        return len(texto_en_bytes).to_bytes(2, "big") + texto_en_bytes

    @staticmethod
    def construir_paquete(tipo_de_paquete, cuerpo):
        longitud_pendiente_de_codificar = len(cuerpo)
        bytes_de_la_longitud = b""

        while True:
            siete_bits_de_la_longitud = longitud_pendiente_de_codificar % 128
            longitud_pendiente_de_codificar //= 128
            hay_mas_bytes_de_longitud = 128 if longitud_pendiente_de_codificar else 0
            bytes_de_la_longitud += bytes([siete_bits_de_la_longitud | hay_mas_bytes_de_longitud])

            if not longitud_pendiente_de_codificar:
                return bytes([tipo_de_paquete]) + bytes_de_la_longitud + cuerpo

    def conectar_con_el_broker(self):
        self.conexion = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=5)

        nombre_del_protocolo = self.codificar_texto_con_su_longitud(b"MQTT")
        version_del_protocolo_y_flags = bytes([4, 2])
        keepalive_desactivado = (0).to_bytes(2, "big")
        identificador = self.codificar_texto_con_su_longitud(self.identificador_del_cliente.encode())

        cuerpo_del_connect = nombre_del_protocolo + version_del_protocolo_y_flags + keepalive_desactivado + identificador
        self.conexion.sendall(self.construir_paquete(0x10, cuerpo_del_connect))

        codigo_de_retorno_del_connack = self.conexion.recv(4)[3]
        if codigo_de_retorno_del_connack != 0:
            raise ConnectionError("MQTT rechazo la conexion")

    def publicar(self, topic, valor):
        paquete = self.construir_paquete(
            0x30,
            self.codificar_texto_con_su_longitud(topic.encode()) + str(valor).encode(),
        )

        for _ in range(2):
            try:
                if self.conexion is None:
                    self.conectar_con_el_broker()
                self.conexion.sendall(paquete)
                return
            except OSError:
                self.conexion = None

        raise ConnectionError(f"sin conexion con MQTT en {MQTT_HOST}:{MQTT_PORT}")


def leer_retrasos_por_viaje_y_anotar_los_viajes_cancelados(nombre_del_fichero_trip_updates, ids_de_viajes_cancelados):
    retraso_en_segundos_por_id_de_viaje = {}

    entidades = descargar_url(f"{URL_BASE_FEEDS_RENFE_TIEMPO_REAL}/{nombre_del_fichero_trip_updates}").get("entity", [])
    for entidad in entidades:
        actualizacion_del_viaje = entidad.get("tripUpdate") or {}
        viaje = actualizacion_del_viaje.get("trip") or {}
        id_de_viaje = viaje.get("tripId")

        if viaje.get("scheduleRelationship") == "CANCELED":
            if id_de_viaje:
                ids_de_viajes_cancelados.append(id_de_viaje)
            continue

        retraso = actualizacion_del_viaje.get("delay")
        if retraso is None:
            for actualizacion_de_parada in actualizacion_del_viaje.get("stopTimeUpdate", []):
                llegada_o_salida = actualizacion_de_parada.get("arrival") or actualizacion_de_parada.get("departure") or {}
                retraso = llegada_o_salida.get("delay")
                if retraso is not None:
                    break

        if id_de_viaje:
            retraso_en_segundos_por_id_de_viaje[id_de_viaje] = None if retraso is None else int(retraso)

    return retraso_en_segundos_por_id_de_viaje


def leer_trenes_de_cercanias(nucleo_por_viaje_de_cercanias, ids_de_viajes_cancelados):
    posiciones_del_feed = descargar_url(f"{URL_BASE_FEEDS_RENFE_TIEMPO_REAL}/vehicle_positions.json")

    try:
        retraso_por_id_de_viaje = leer_retrasos_por_viaje_y_anotar_los_viajes_cancelados(
            "trip_updates.json", ids_de_viajes_cancelados
        )
    except Exception as error:
        print(f"Aviso: sin retrasos de Cercanias ({error})", flush=True)
        retraso_por_id_de_viaje = {}

    for entidad in posiciones_del_feed.get("entity", []):
        vehiculo_gtfs_rt = entidad["vehicle"]
        posicion = vehiculo_gtfs_rt.get("position") or {}
        id_de_viaje = (vehiculo_gtfs_rt.get("trip") or {}).get("tripId", "")

        if "latitude" not in posicion or "longitude" not in posicion:
            continue

        etiqueta_del_vehiculo = (vehiculo_gtfs_rt.get("vehicle") or {}).get("label") or entidad["id"]
        codigo_del_tren = (vehiculo_gtfs_rt.get("vehicle") or {}).get("id") or etiqueta_del_vehiculo
        timestamp_de_la_posicion = int(vehiculo_gtfs_rt.get("timestamp") or posiciones_del_feed["header"]["timestamp"])

        yield {
            "tren": codigo_del_tren,
            "linea": etiqueta_del_vehiculo.split("-")[0] or "?",
            "nucleo": NUCLEO_POR_PREFIJO_DE_RUTA_DE_CERCANIAS.get(
                nucleo_por_viaje_de_cercanias.get(id_de_viaje, ""), "Desconocido"
            ),
            "categoria": "Cercanias",
            "id_de_viaje": id_de_viaje,
            "timestamp_de_la_posicion": timestamp_de_la_posicion,
            "vehiculo_gtfs_rt": vehiculo_gtfs_rt,
            "retraso_en_segundos": retraso_por_id_de_viaje.get(id_de_viaje),
            "es_larga_distancia": False,
        }


def leer_trenes_de_larga_distancia(tipo_de_servicio_por_numero_de_tren, ids_de_viajes_cancelados):
    posiciones_del_feed = descargar_url(f"{URL_BASE_FEEDS_RENFE_TIEMPO_REAL}/vehicle_positions_LD.json")

    try:
        retraso_por_id_de_viaje = leer_retrasos_por_viaje_y_anotar_los_viajes_cancelados(
            "trip_updates_LD.json", ids_de_viajes_cancelados
        )
    except Exception as error:
        print(f"Aviso: sin retrasos de LD ({error})", flush=True)
        retraso_por_id_de_viaje = {}

    instante_de_recepcion = int(time.time())

    for entidad in posiciones_del_feed.get("entity", []):
        vehiculo_gtfs_rt = entidad["vehicle"]
        posicion = vehiculo_gtfs_rt.get("position") or {}
        id_de_viaje = (vehiculo_gtfs_rt.get("trip") or {}).get("tripId", "")
        numero_de_tren = (vehiculo_gtfs_rt.get("vehicle") or {}).get("id") or id_de_viaje[:5]

        if "latitude" not in posicion or "longitude" not in posicion:
            continue
        if not tipo_de_servicio_por_numero_de_tren.get(numero_de_tren):
            continue

        tipo_de_servicio = tipo_de_servicio_por_numero_de_tren[numero_de_tren]

        yield {
            "tren": numero_de_tren,
            "linea": tipo_de_servicio,
            "nucleo": NOMBRE_DEL_NUCLEO_DE_LARGA_DISTANCIA,
            "categoria": CATEGORIA_POR_TIPO_DE_SERVICIO_DE_LARGA_DISTANCIA.get(
                tipo_de_servicio, CATEGORIA_SI_EL_TIPO_DE_SERVICIO_NO_ESTA_EN_LA_TABLA
            ),
            "id_de_viaje": id_de_viaje,
            "timestamp_de_la_posicion": instante_de_recepcion,
            "vehiculo_gtfs_rt": vehiculo_gtfs_rt,
            "retraso_en_segundos": retraso_por_id_de_viaje.get(id_de_viaje),
            "es_larga_distancia": True,
        }


def construir_mensaje_de_viaje_cancelado(id_de_viaje, es_larga_distancia, gtfs):
    if es_larga_distancia:
        numero_de_tren = id_de_viaje[:5]
        tipo_de_servicio = gtfs.tipo_de_servicio_por_numero_de_tren_de_larga_distancia.get(numero_de_tren)
        if not tipo_de_servicio:
            return None

        mensaje = {
            "tren": numero_de_tren,
            "linea": tipo_de_servicio,
            "nucleo": NOMBRE_DEL_NUCLEO_DE_LARGA_DISTANCIA,
            "categoria": CATEGORIA_POR_TIPO_DE_SERVICIO_DE_LARGA_DISTANCIA.get(
                tipo_de_servicio, CATEGORIA_SI_EL_TIPO_DE_SERVICIO_NO_ESTA_EN_LA_TABLA
            ),
            "trip": id_de_viaje,
        }

        id_de_parada_origen, id_de_parada_destino = (
            gtfs.primera_y_ultima_parada_por_numero_de_tren_de_larga_distancia.get(numero_de_tren, ("", ""))
        )
        if id_de_parada_origen in gtfs.nombre_por_id_de_parada:
            mensaje["origen"] = gtfs.nombre_por_id_de_parada[id_de_parada_origen]
        if id_de_parada_destino in gtfs.nombre_por_id_de_parada:
            mensaje["destino"] = gtfs.nombre_por_id_de_parada[id_de_parada_destino]
        if id_de_parada_origen in gtfs.coordenadas_por_id_de_parada_de_larga_distancia:
            mensaje["origen_lat"], mensaje["origen_lon"] = gtfs.coordenadas_por_id_de_parada_de_larga_distancia[
                id_de_parada_origen
            ]

        return mensaje

    partes_del_id_de_viaje = re.match(
        r"(?P<prefijo_de_nucleo>\d{2})\d*X(?P<numero_de_tren>\d+)(?P<linea>\D\w*)$", id_de_viaje
    )
    if not partes_del_id_de_viaje:
        return None

    prefijo_de_nucleo = gtfs.nucleo_por_viaje_de_cercanias.get(id_de_viaje) or partes_del_id_de_viaje["prefijo_de_nucleo"]

    return {
        "tren": partes_del_id_de_viaje["numero_de_tren"],
        "linea": partes_del_id_de_viaje["linea"],
        "categoria": "Cercanias",
        "trip": id_de_viaje,
        "nucleo": NUCLEO_POR_PREFIJO_DE_RUTA_DE_CERCANIAS.get(prefijo_de_nucleo, "Desconocido"),
    }


def reunir_trenes_y_viajes_cancelados_de_los_dos_feeds(gtfs):
    trenes = []
    ids_de_viajes_cancelados_por_feed = {}

    feeds = (
        ("Cercanias", leer_trenes_de_cercanias, gtfs.nucleo_por_viaje_de_cercanias),
        ("LD", leer_trenes_de_larga_distancia, gtfs.tipo_de_servicio_por_numero_de_tren_de_larga_distancia),
    )
    for nombre_del_feed, leer_trenes_del_feed, datos_gtfs_del_feed in feeds:
        ids_de_viajes_cancelados_por_feed[nombre_del_feed] = []
        try:
            trenes += list(leer_trenes_del_feed(datos_gtfs_del_feed, ids_de_viajes_cancelados_por_feed[nombre_del_feed]))
        except Exception as error:
            print(f"Aviso: feed de {nombre_del_feed} no disponible ({error})", flush=True)

    return trenes, ids_de_viajes_cancelados_por_feed


def tren_sigue_en_la_misma_posicion_que_la_ultima_publicada(tren, ultima_publicada, instante_actual):
    if tren["es_larga_distancia"]:
        posicion = tren["vehiculo_gtfs_rt"]["position"]
        no_se_ha_movido = (ultima_publicada.latitud, ultima_publicada.longitud) == (
            posicion["latitude"],
            posicion["longitude"],
        )
        segundos_desde_la_ultima_publicacion = instante_actual - ultima_publicada.instante_en_que_se_publico
        return no_se_ha_movido and segundos_desde_la_ultima_publicacion < SEGUNDOS_PARA_REPUBLICAR_UN_TREN_DE_LARGA_DISTANCIA_QUIETO

    return ultima_publicada.timestamp_de_la_posicion == tren["timestamp_de_la_posicion"]


def estimar_velocidad_en_kmh_desde_la_ultima_posicion(tren, ultima_publicada):
    segundos_entre_posiciones = tren["timestamp_de_la_posicion"] - ultima_publicada.timestamp_de_la_posicion
    if not 10 <= segundos_entre_posiciones <= 300:
        return None

    posicion = tren["vehiculo_gtfs_rt"]["position"]
    distancia_en_km = calcular_distancia_en_km_entre_dos_coordenadas(
        ultima_publicada.latitud, ultima_publicada.longitud, posicion["latitude"], posicion["longitude"]
    )
    velocidad_en_kmh = distancia_en_km / segundos_entre_posiciones * 3600

    velocidad_maxima_creible = VELOCIDAD_MAXIMA_CREIBLE_EN_KMH_POR_CATEGORIA.get(
        tren["categoria"], VELOCIDAD_MAXIMA_CREIBLE_EN_KMH_SI_LA_CATEGORIA_NO_ESTA_EN_LA_TABLA
    )
    if velocidad_en_kmh > velocidad_maxima_creible:
        return None

    return round(velocidad_en_kmh, 1)


def construir_mensaje_de_posicion_de_tren(tren, ultima_publicada, nombre_por_id_de_parada):
    vehiculo_gtfs_rt = tren["vehiculo_gtfs_rt"]
    posicion = vehiculo_gtfs_rt["position"]
    id_de_la_proxima_parada = vehiculo_gtfs_rt.get("stopId", "")

    mensaje = {
        "tren": tren["tren"],
        "linea": tren["linea"],
        "nucleo": tren["nucleo"],
        "categoria": tren["categoria"],
        "trip": tren["id_de_viaje"],
        "estado": vehiculo_gtfs_rt.get("currentStatus", ""),
        "parada_id": id_de_la_proxima_parada,
        "parada": nombre_por_id_de_parada.get(id_de_la_proxima_parada, ""),
        "lat": posicion["latitude"],
        "lon": posicion["longitude"],
        "time": tren["timestamp_de_la_posicion"] * 1000,
    }

    if tren["retraso_en_segundos"] is not None:
        mensaje["retraso"] = tren["retraso_en_segundos"]

    if ultima_publicada:
        velocidad_en_kmh = estimar_velocidad_en_kmh_desde_la_ultima_posicion(tren, ultima_publicada)
        if velocidad_en_kmh is not None:
            mensaje["vel"] = velocidad_en_kmh

    return mensaje


def publicar_las_posiciones_que_han_cambiado(
    cliente_mqtt, trenes, instante_actual, ultima_posicion_publicada_por_tren, nombre_por_id_de_parada
):
    cuantas_posiciones_nuevas = 0
    trenes_activos = []

    for tren in trenes:
        posicion = tren["vehiculo_gtfs_rt"]["position"]
        timestamp_de_la_posicion = tren["timestamp_de_la_posicion"]

        tren_activo = (
            tren["es_larga_distancia"]
            or instante_actual - timestamp_de_la_posicion
            <= SEGUNDOS_SIN_POSICION_NUEVA_PARA_DAR_UN_TREN_DE_CERCANIAS_POR_INACTIVO
        )
        if tren_activo:
            trenes_activos.append((tren["nucleo"], tren["retraso_en_segundos"]))

        clave_del_tren = (tren["nucleo"], tren["linea"], tren["tren"])
        ultima_publicada = ultima_posicion_publicada_por_tren.get(clave_del_tren)

        if ultima_publicada and tren_sigue_en_la_misma_posicion_que_la_ultima_publicada(
            tren, ultima_publicada, instante_actual
        ):
            continue

        mensaje = construir_mensaje_de_posicion_de_tren(tren, ultima_publicada, nombre_por_id_de_parada)

        ultima_posicion_publicada_por_tren[clave_del_tren] = UltimaPosicionPublicada(
            timestamp_de_la_posicion, posicion["latitude"], posicion["longitude"], instante_actual
        )
        cliente_mqtt.publicar(f"{TOPIC_BASE_MQTT}/trenes", json.dumps(mensaje))
        cuantas_posiciones_nuevas += 1

    return cuantas_posiciones_nuevas, trenes_activos


def calcular_retraso_medio_y_cuantos_van_retrasados(retrasos_en_segundos):
    retrasos_conocidos = [retraso for retraso in retrasos_en_segundos if retraso is not None]

    retraso_medio = round(sum(retrasos_conocidos) / len(retrasos_conocidos), 1) if retrasos_conocidos else 0
    con_retraso = sum(1 for retraso in retrasos_conocidos if retraso >= SEGUNDOS_DE_RETRASO_PARA_CONTAR_COMO_RETRASADO)

    return retraso_medio, con_retraso


def publicar_estadisticas_globales(cliente_mqtt, trenes_activos):
    retraso_medio, con_retraso = calcular_retraso_medio_y_cuantos_van_retrasados(
        [retraso for _, retraso in trenes_activos]
    )
    estadisticas = {
        "trenes_activos": len(trenes_activos),
        "retraso_medio_s": retraso_medio,
        "con_retraso": con_retraso,
    }

    for nombre_de_la_estadistica, valor in estadisticas.items():
        cliente_mqtt.publicar(f"{TOPIC_BASE_MQTT}/stats/{nombre_de_la_estadistica}", valor)

    return estadisticas


def publicar_estadisticas_por_nucleo(cliente_mqtt, trenes_activos):
    retrasos_por_nucleo = defaultdict(list)
    for nucleo, retraso in trenes_activos:
        retrasos_por_nucleo[nucleo].append(retraso)

    for nucleo, retrasos in retrasos_por_nucleo.items():
        retraso_medio, con_retraso = calcular_retraso_medio_y_cuantos_van_retrasados(retrasos)
        cliente_mqtt.publicar(
            f"{TOPIC_BASE_MQTT}/nucleos",
            json.dumps({
                "nucleo": nucleo,
                "trenes": len(retrasos),
                "retraso_medio": retraso_medio,
                "con_retraso": con_retraso,
            }),
        )

    return retrasos_por_nucleo


def olvidar_los_trenes_no_vistos_desde_hace_tiempo(ultima_posicion_publicada_por_tren, instante_actual):
    for clave_del_tren, ultima_publicada in list(ultima_posicion_publicada_por_tren.items()):
        if instante_actual - ultima_publicada.instante_en_que_se_publico > SEGUNDOS_PARA_OLVIDAR_UN_TREN_NO_VISTO:
            del ultima_posicion_publicada_por_tren[clave_del_tren]


def publicar_los_viajes_cancelados_y_olvidar_los_que_ya_no_estan(
    cliente_mqtt, ids_de_viajes_cancelados_por_feed, instante_actual, ultima_publicacion_por_viaje_cancelado, gtfs
):
    ids_de_viajes_cancelados_vigentes = set()

    for nombre_del_feed, ids_de_viajes_cancelados in ids_de_viajes_cancelados_por_feed.items():
        for id_de_viaje in ids_de_viajes_cancelados:
            mensaje = construir_mensaje_de_viaje_cancelado(id_de_viaje, nombre_del_feed == "LD", gtfs)
            if not mensaje:
                continue

            ids_de_viajes_cancelados_vigentes.add(id_de_viaje)

            segundos_desde_la_ultima_publicacion = instante_actual - ultima_publicacion_por_viaje_cancelado.get(id_de_viaje, 0)
            if segundos_desde_la_ultima_publicacion >= SEGUNDOS_PARA_REPUBLICAR_UN_VIAJE_CANCELADO:
                cliente_mqtt.publicar(f"{TOPIC_BASE_MQTT}/cancelados", json.dumps(mensaje))
                ultima_publicacion_por_viaje_cancelado[id_de_viaje] = instante_actual

    for id_de_viaje in list(ultima_publicacion_por_viaje_cancelado):
        if id_de_viaje not in ids_de_viajes_cancelados_vigentes:
            del ultima_publicacion_por_viaje_cancelado[id_de_viaje]

    return len(ids_de_viajes_cancelados_vigentes)


def consultar_renfe_y_publicar_en_mqtt_para_siempre():
    gtfs = cargar_gtfs_estaticos_de_cercanias_y_larga_distancia()
    cliente_mqtt = ClienteMqttMinimo()
    ultima_posicion_publicada_por_tren = {}
    ultima_publicacion_por_viaje_cancelado = {}

    print(f"Consultando Renfe cada {SEGUNDOS_ENTRE_CONSULTAS_A_RENFE}s  (Ctrl+C para parar)", flush=True)

    while True:
        instante_de_inicio_de_la_consulta = time.time()

        try:
            instante_actual = time.time()

            trenes, ids_de_viajes_cancelados_por_feed = reunir_trenes_y_viajes_cancelados_de_los_dos_feeds(gtfs)

            cuantas_posiciones_nuevas, trenes_activos = publicar_las_posiciones_que_han_cambiado(
                cliente_mqtt,
                trenes,
                instante_actual,
                ultima_posicion_publicada_por_tren,
                gtfs.nombre_por_id_de_parada,
            )

            estadisticas = publicar_estadisticas_globales(cliente_mqtt, trenes_activos)
            retrasos_por_nucleo = publicar_estadisticas_por_nucleo(cliente_mqtt, trenes_activos)

            olvidar_los_trenes_no_vistos_desde_hace_tiempo(ultima_posicion_publicada_por_tren, instante_actual)

            cuantos_viajes_cancelados = publicar_los_viajes_cancelados_y_olvidar_los_que_ya_no_estan(
                cliente_mqtt,
                ids_de_viajes_cancelados_por_feed,
                instante_actual,
                ultima_publicacion_por_viaje_cancelado,
                gtfs,
            )

            trenes_activos_de_larga_distancia = len(retrasos_por_nucleo.get(NOMBRE_DEL_NUCLEO_DE_LARGA_DISTANCIA, []))
            print(
                f"[{time.strftime('%H:%M:%S')}] posiciones nuevas: {cuantas_posiciones_nuevas:>3}   "
                f"trenes activos: {estadisticas['trenes_activos']:>3} (LD {trenes_activos_de_larga_distancia})   "
                f"retraso medio: {estadisticas['retraso_medio_s']:>6}s   "
                f"con retraso: {estadisticas['con_retraso']:>3}   cancelados: {cuantos_viajes_cancelados:>3}",
                flush=True,
            )

        except ConnectionError as error:
            print(f"MQTT: {error}", flush=True)
        except Exception as error:
            print(f"Error: {error}", flush=True)

        segundos_que_ya_ha_durado_la_consulta = time.time() - instante_de_inicio_de_la_consulta
        time.sleep(max(0, SEGUNDOS_ENTRE_CONSULTAS_A_RENFE - segundos_que_ya_ha_durado_la_consulta))


if __name__ == "__main__":
    try:
        consultar_renfe_y_publicar_en_mqtt_para_siempre()
    except KeyboardInterrupt:
        print("\nDetenido.")
