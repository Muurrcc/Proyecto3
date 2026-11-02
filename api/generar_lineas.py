import json
import os
import zipfile
from collections import defaultdict

from trenes import (
    NUCLEO_POR_PREFIJO_DE_RUTA_DE_CERCANIAS,
    RUTA_CACHE_GTFS_CERCANIAS,
    URL_GTFS_CERCANIAS,
    descargar_url,
    leer_filas_de_un_csv_dentro_del_zip,
)

RUTA_DEL_GEOJSON_DE_SALIDA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "web", "static", "lineas.geojson"
)
TOLERANCIA_DE_SIMPLIFICACION_EN_GRADOS = 0.0003


def calcular_distancia_de_un_punto_a_un_segmento(punto, inicio_del_segmento, fin_del_segmento):
    x, y = punto
    x_inicio, y_inicio = inicio_del_segmento
    x_fin, y_fin = fin_del_segmento

    ancho_del_segmento = x_fin - x_inicio
    alto_del_segmento = y_fin - y_inicio

    if ancho_del_segmento == alto_del_segmento == 0:
        return ((x - x_inicio) ** 2 + (y - y_inicio) ** 2) ** 0.5

    posicion_del_punto_proyectado_en_el_segmento = max(0, min(1, (
        (x - x_inicio) * ancho_del_segmento + (y - y_inicio) * alto_del_segmento
    ) / (ancho_del_segmento ** 2 + alto_del_segmento ** 2)))

    x_proyectado = x_inicio + posicion_del_punto_proyectado_en_el_segmento * ancho_del_segmento
    y_proyectado = y_inicio + posicion_del_punto_proyectado_en_el_segmento * alto_del_segmento

    return ((x - x_proyectado) ** 2 + (y - y_proyectado) ** 2) ** 0.5


def simplificar_trazado_con_ramer_douglas_peucker(puntos, tolerancia):
    if len(puntos) < 3:
        return puntos

    puntos_a_mantener = [False] * len(puntos)
    puntos_a_mantener[0] = puntos_a_mantener[-1] = True

    tramos_pendientes = [(0, len(puntos) - 1)]
    while tramos_pendientes:
        indice_inicial, indice_final = tramos_pendientes.pop()

        distancia_maxima = 0
        indice_del_punto_mas_lejano = None
        for indice in range(indice_inicial + 1, indice_final):
            distancia = calcular_distancia_de_un_punto_a_un_segmento(
                puntos[indice], puntos[indice_inicial], puntos[indice_final]
            )
            if distancia > distancia_maxima:
                distancia_maxima = distancia
                indice_del_punto_mas_lejano = indice

        if indice_del_punto_mas_lejano is not None and distancia_maxima > tolerancia:
            puntos_a_mantener[indice_del_punto_mas_lejano] = True
            tramos_pendientes += [
                (indice_inicial, indice_del_punto_mas_lejano),
                (indice_del_punto_mas_lejano, indice_final),
            ]

    return [punto for punto, mantener in zip(puntos, puntos_a_mantener) if mantener]


def descargar_gtfs_de_cercanias_si_no_esta_en_la_cache():
    if os.path.exists(RUTA_CACHE_GTFS_CERCANIAS):
        return

    os.makedirs(os.path.dirname(RUTA_CACHE_GTFS_CERCANIAS), exist_ok=True)
    print("Descargando GTFS estatico de Renfe (~16 MB)...")
    with open(RUTA_CACHE_GTFS_CERCANIAS, "wb") as fichero:
        fichero.write(descargar_url(URL_GTFS_CERCANIAS, como_bytes=True))


def leer_rutas_y_puntos_de_cada_trazado_del_gtfs():
    with zipfile.ZipFile(RUTA_CACHE_GTFS_CERCANIAS) as gtfs:
        rutas_por_id = {ruta["route_id"]: ruta for ruta in leer_filas_de_un_csv_dentro_del_zip(gtfs, "routes.txt")}

        id_de_ruta_por_id_de_trazado = {}
        for viaje in leer_filas_de_un_csv_dentro_del_zip(gtfs, "trips.txt"):
            id_de_ruta_por_id_de_trazado.setdefault(viaje["shape_id"], viaje["route_id"])

        puntos_por_id_de_trazado = defaultdict(list)
        for punto in leer_filas_de_un_csv_dentro_del_zip(gtfs, "shapes.txt"):
            es_el_sentido_contrario_con_el_mismo_trazado = punto["shape_id"].endswith("_INV")
            if es_el_sentido_contrario_con_el_mismo_trazado:
                continue

            puntos_por_id_de_trazado[punto["shape_id"]].append((
                int(punto["shape_pt_sequence"]),
                float(punto["shape_pt_lon"]),
                float(punto["shape_pt_lat"]),
            ))

    return rutas_por_id, id_de_ruta_por_id_de_trazado, puntos_por_id_de_trazado


def construir_geojson_de_las_lineas_de_cercanias():
    rutas_por_id, id_de_ruta_por_id_de_trazado, puntos_por_id_de_trazado = leer_rutas_y_puntos_de_cada_trazado_del_gtfs()

    lineas_geojson = []
    for id_de_trazado, puntos_con_secuencia in sorted(puntos_por_id_de_trazado.items()):
        ruta = rutas_por_id.get(id_de_ruta_por_id_de_trazado.get(id_de_trazado, ""))
        if not ruta:
            continue

        puntos_ordenados = [(longitud, latitud) for _, longitud, latitud in sorted(puntos_con_secuencia)]
        coordenadas_simplificadas = [
            [round(longitud, 5), round(latitud, 5)]
            for longitud, latitud in simplificar_trazado_con_ramer_douglas_peucker(
                puntos_ordenados, TOLERANCIA_DE_SIMPLIFICACION_EN_GRADOS
            )
        ]

        lineas_geojson.append({
            "type": "Feature",
            "properties": {
                "linea": ruta["route_short_name"],
                "nucleo": NUCLEO_POR_PREFIJO_DE_RUTA_DE_CERCANIAS.get(ruta["route_id"][:2], "Desconocido"),
                "color": "#" + (ruta["route_color"] or "888888"),
            },
            "geometry": {"type": "LineString", "coordinates": coordenadas_simplificadas},
        })

    return lineas_geojson


def guardar_geojson_de_las_lineas_de_cercanias():
    descargar_gtfs_de_cercanias_si_no_esta_en_la_cache()
    lineas_geojson = construir_geojson_de_las_lineas_de_cercanias()

    os.makedirs(os.path.dirname(RUTA_DEL_GEOJSON_DE_SALIDA), exist_ok=True)
    with open(RUTA_DEL_GEOJSON_DE_SALIDA, "w") as fichero:
        json.dump({"type": "FeatureCollection", "features": lineas_geojson}, fichero, separators=(",", ":"))

    total_de_puntos = sum(len(linea["geometry"]["coordinates"]) for linea in lineas_geojson)
    tamano_en_kb = os.path.getsize(RUTA_DEL_GEOJSON_DE_SALIDA) / 1024
    print(f"{len(lineas_geojson)} lineas, {total_de_puntos} puntos -> {tamano_en_kb:.0f} KB")


if __name__ == "__main__":
    guardar_geojson_de_las_lineas_de_cercanias()
