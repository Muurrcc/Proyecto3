import json
import os
import time
import urllib.parse
import urllib.request

from generar_lineas import simplificar_trazado_con_ramer_douglas_peucker

URL_DE_OVERPASS = "https://overpass-api.de/api/interpreter"
RUTA_DEL_GEOJSON_DE_SALIDA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "web", "static", "red.geojson"
)
TOLERANCIA_DE_SIMPLIFICACION_EN_GRADOS = 0.0008

LATITUDES_DE_LAS_CUADRICULAS = [36.0, 38.4, 40.8, 43.9]
LONGITUDES_DE_LAS_CUADRICULAS = [-9.5, -6.5, -3.5, -0.5, 3.5]
CUADRICULAS_SUR_OESTE_NORTE_ESTE = [
    (
        LATITUDES_DE_LAS_CUADRICULAS[fila],
        LONGITUDES_DE_LAS_CUADRICULAS[columna],
        LATITUDES_DE_LAS_CUADRICULAS[fila + 1],
        LONGITUDES_DE_LAS_CUADRICULAS[columna + 1],
    )
    for fila in range(len(LATITUDES_DE_LAS_CUADRICULAS) - 1)
    for columna in range(len(LONGITUDES_DE_LAS_CUADRICULAS) - 1)
]

INTENTOS_MAXIMOS_POR_CUADRICULA = 6
SEGUNDOS_DE_ESPERA_ENTRE_CUADRICULAS = 3


def descargar_vias_de_una_cuadricula_desde_overpass(cuadricula):
    sur, oeste, norte, este = cuadricula
    consulta_overpass = (
        '[out:json][timeout:120];area["ISO3166-1"="ES"][admin_level=2]->.es;'
        f'way["railway"="rail"]["usage"="main"]["service"!~"."]({sur},{oeste},{norte},{este})(area.es);out geom qt;'
    )

    segundos_de_espera_tras_un_fallo = 10
    for numero_de_intento in range(1, INTENTOS_MAXIMOS_POR_CUADRICULA + 1):
        try:
            peticion = urllib.request.Request(
                URL_DE_OVERPASS,
                data=urllib.parse.urlencode({"data": consulta_overpass}).encode(),
                headers={"User-Agent": "practica3-renfe/1.0 (proyecto de clase)"},
            )
            with urllib.request.urlopen(peticion, timeout=180) as respuesta:
                return json.load(respuesta)["elements"]
        except Exception as error:
            print(
                f"  intento {numero_de_intento} fallido ({error}); espero {segundos_de_espera_tras_un_fallo}s",
                flush=True,
            )
            time.sleep(segundos_de_espera_tras_un_fallo)
            segundos_de_espera_tras_un_fallo *= 2

    raise RuntimeError(f"Overpass no responde para {cuadricula}")


def descargar_y_simplificar_las_vias_de_toda_la_peninsula():
    vias_por_id_de_osm = {}

    for numero_de_cuadricula, cuadricula in enumerate(CUADRICULAS_SUR_OESTE_NORTE_ESTE, 1):
        vias_descargadas = descargar_vias_de_una_cuadricula_desde_overpass(cuadricula)

        vias_nuevas = 0
        for via in vias_descargadas:
            via_ya_descargada_o_sin_geometria = via["id"] in vias_por_id_de_osm or not via.get("geometry")
            if via_ya_descargada_o_sin_geometria:
                continue

            es_alta_velocidad = via.get("tags", {}).get("highspeed") == "yes"
            puntos = [(punto["lon"], punto["lat"]) for punto in via["geometry"]]
            puntos_simplificados = simplificar_trazado_con_ramer_douglas_peucker(
                puntos, TOLERANCIA_DE_SIMPLIFICACION_EN_GRADOS
            )

            vias_por_id_de_osm[via["id"]] = (
                es_alta_velocidad,
                [[round(longitud, 5), round(latitud, 5)] for longitud, latitud in puntos_simplificados],
            )
            vias_nuevas += 1

        print(
            f"cuadricula {numero_de_cuadricula}/{len(CUADRICULAS_SUR_OESTE_NORTE_ESTE)} {cuadricula}: "
            f"{len(vias_descargadas)} vias, {vias_nuevas} nuevas",
            flush=True,
        )
        time.sleep(SEGUNDOS_DE_ESPERA_ENTRE_CUADRICULAS)

    return vias_por_id_de_osm


def construir_geojson_con_vias_convencionales_y_de_alta_velocidad(vias_por_id_de_osm):
    tipos_de_via = ((False, "convencional"), (True, "alta_velocidad"))

    elementos_geojson = []
    for es_alta_velocidad, nombre_del_tipo in tipos_de_via:
        trazados_del_tipo = [
            trazado
            for via_es_alta_velocidad, trazado in vias_por_id_de_osm.values()
            if via_es_alta_velocidad == es_alta_velocidad and len(trazado) > 1
        ]
        elementos_geojson.append({
            "type": "Feature",
            "properties": {"tipo": nombre_del_tipo},
            "geometry": {"type": "MultiLineString", "coordinates": trazados_del_tipo},
        })

    return elementos_geojson


def guardar_geojson_de_la_red_ferroviaria():
    vias_por_id_de_osm = descargar_y_simplificar_las_vias_de_toda_la_peninsula()
    elementos_geojson = construir_geojson_con_vias_convencionales_y_de_alta_velocidad(vias_por_id_de_osm)

    os.makedirs(os.path.dirname(RUTA_DEL_GEOJSON_DE_SALIDA), exist_ok=True)
    with open(RUTA_DEL_GEOJSON_DE_SALIDA, "w") as fichero:
        json.dump({"type": "FeatureCollection", "features": elementos_geojson}, fichero, separators=(",", ":"))

    total_de_tramos = sum(len(elemento["geometry"]["coordinates"]) for elemento in elementos_geojson)
    tamano_en_kb = os.path.getsize(RUTA_DEL_GEOJSON_DE_SALIDA) / 1024
    print(f"{total_de_tramos} tramos -> {tamano_en_kb:.0f} KB", flush=True)


if __name__ == "__main__":
    guardar_geojson_de_la_red_ferroviaria()
