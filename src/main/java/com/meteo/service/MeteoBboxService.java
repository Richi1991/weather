package com.meteo.service;

import com.meteo.client.OpenMeteoClient;
import com.meteo.common.Constantes;
import com.meteo.model.OpenMeteoResponse;
import com.meteo.model.PuntoMalla;
import com.meteo.repository.RedisCurrentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class MeteoBboxService {

    private static final Logger log = LoggerFactory.getLogger(MeteoBboxService.class);

    @Value("${meteo.openmeteo.paso-malla}")
    private double pasoMalla;  // 0.25 grados

    @Autowired
    private OpenMeteoClient openMeteoClient;

    @Autowired
    private MeteoQueryService queryService;

    @Autowired
    private RedisCurrentRepository redisCurrentRepository;

    /**
     * Punto de entrada principal desde el controller.
     *
     * Recibe el bbox que el usuario tiene visible en el mapa y devuelve
     * una lista de puntos con precipitación para pintar el heatmap.
     *
     * Estrategia:
     *   - Si el bbox está dentro de España → sirve desde BD (rápido, sin llamadas externas).
     *   - Si está fuera de España → llama a Open-Meteo y cachea 1h.
     *   - Si el bbox es muy grande (zoom out) → aumenta el paso de malla para no
     *     generar miles de puntos (máx ~200 puntos por respuesta).
     */
    public List<Map<String, Object>> getGridPrecipitacionBbox(
            double latMin, double latMax, double lonMin, double lonMax) {

        if (esDentroEspana(latMin, latMax, lonMin, lonMax)) {
            log.debug("Bbox dentro de España, sirviendo desde BD");
            return queryService.getGridPrecipitacionBbox(latMin, latMax, lonMin, lonMax);
        }

        log.debug("Bbox mundial [{},{}] [{},{}], usando proxy Open-Meteo",
                latMin, latMax, lonMin, lonMax);
        return getGridMundialCacheado(
                redondearBbox(latMin),
                redondearBbox(latMax),
                redondearBbox(lonMin),
                redondearBbox(lonMax)
        );
    }

    public List<Map<String, Object>> getGridCurrentBbox(
            double latMin, double latMax, double lonMin, double lonMax) {

        if (esDentroEspana(latMin, latMax, lonMin, lonMax)) {
            return redisCurrentRepository.getCurrentBbox(latMin, latMax, lonMin, lonMax);
        }

        return getGridCurrentMundialCacheado(
                redondearBbox(latMin),
                redondearBbox(latMax),
                redondearBbox(lonMin),
                redondearBbox(lonMax)
        );
    }

    /**
     * Llama a Open-Meteo para el bbox dado y cachea el resultado 1 hora.
     *
     * La clave de cache usa el bbox redondeado a la malla (0.75°) para que
     * desplazamientos pequeños del mapa reutilicen la misma entrada de cache.
     */
    @Cacheable(value = "bbox-mundial",
               key = "#latMin + ':' + #latMax + ':' + #lonMin + ':' + #lonMax")
    public List<Map<String, Object>> getGridMundialCacheado(
            double latMin, double latMax, double lonMin, double lonMax) {

        List<PuntoMalla> puntos = generarPuntosMalla(latMin, latMax, lonMin, lonMax);
        if (puntos.isEmpty()) return List.of();

        log.debug("Cache miss - llamando Open-Meteo para {} puntos", puntos.size());

        List<Map<String, Object>> resultado = new ArrayList<>();
        List<List<PuntoMalla>> batches = dividirEnBatches(puntos, Constantes.BATCH_SIZE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (List<PuntoMalla> batch : batches) {
            var future = openMeteoClient
                    .getForecastBatch(batch)
                    .thenAccept(responses -> {
                        for (int i = 0; i < responses.length; i++) {
                            var r = responses[i];
                            if (r == null || r.daily() == null
                                    || r.daily().precipitationSum() == null
                                    || r.daily().precipitationSum().isEmpty()) continue;

                            var punto = batch.get(i);
                            Map<String, Object> fila = new HashMap<>();
                            fila.put("latitud",          punto.getLatitud());
                            fila.put("longitud",         punto.getLongitud());
                            // Precipitación del día de hoy (primer elemento)
                            fila.put("precipitation_sum", r.daily().precipitationSum().get(0));
                            synchronized (resultado) {
                                resultado.add(fila);
                            }
                        }
                    })
                    .exceptionally(e -> {
                        log.error("Error en batch bbox: {}", e.getMessage());
                        return null;
                    });
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        return resultado;
    }

    @Cacheable(value = "bbox-current",
            key = "#latMin + ':' + #latMax + ':' + #lonMin + ':' + #lonMax")
    public List<Map<String, Object>> getGridCurrentMundialCacheado(
            double latMin, double latMax, double lonMin, double lonMax) {

        List<PuntoMalla> puntos = generarPuntosMalla(latMin, latMax, lonMin, lonMax);
        if (puntos.isEmpty()) return List.of();

        List<Map<String, Object>> resultado = new ArrayList<>();
        List<List<PuntoMalla>> batches = dividirEnBatches(puntos, Constantes.BATCH_SIZE);

        for (List<PuntoMalla> batch : batches) {
            try {
                OpenMeteoResponse[] responses = openMeteoClient
                        .getCurrentBatch(batch)
                        .get();

                for (int i = 0; i < responses.length; i++) {
                    var r = responses[i];
                    if (r == null || r.current() == null) continue;

                    var punto = batch.get(i);
                    Map<String, Object> fila = new HashMap<>();
                    fila.put("latitud",        punto.getLatitud());
                    fila.put("longitud",       punto.getLongitud());
                    fila.put("temperature_2m", r.current().temperature2m());
                    fila.put("weathercode",    r.current().weathercode());
                    fila.put("windspeed_10m",  r.current().windspeed());
                    fila.put("precipitation",  r.current().precipitation());
                    synchronized (resultado) { resultado.add(fila); }
                }
                Thread.sleep(300);
            } catch (Exception e) {
                log.error("Error en batch current mundial: {}", e.getMessage());
            }
        }
        return resultado;
    }

    @Cacheable(value = "grid-espana", key = "#fecha.toString()")
    public List<Map<String, Object>> getGridEspanaCacheado(LocalDate fecha) {
        return queryService.getGridPrecipitacion(fecha);
    }

    /**
     * Genera los puntos de la malla para un bbox dado.
     *
     * Adapta el paso de malla automáticamente según el tamaño del bbox
     * para no superar ~200 puntos (evita respuestas enormes y límites de API).
     *
     * Ejemplos:
     *   - Zoom ciudad (2°×2°):   paso 0.75° →  ~9 puntos   (resolución alta)
     *   - Zoom país (10°×10°):   paso 0.75° → ~196 puntos  (resolución alta)
     *   - Zoom continente (40°×40°): paso auto ~3° → ~196 puntos (resolución reducida)
     *   - Zoom mundial (180°×360°):  paso auto ~15° → ~180 puntos
     */
    public List<PuntoMalla> generarPuntosMalla(
            double latMin, double latMax, double lonMin, double lonMax) {

        double paso = calcularPasoAdaptativo(latMax - latMin, lonMax - lonMin);

        // Alinear al grid de Open-Meteo (múltiplos de pasoMalla desde 0)
        double latIni = Math.ceil(latMin  / paso) * paso;
        double lonIni = Math.ceil(lonMin  / paso) * paso;

        List<PuntoMalla> puntos = new ArrayList<>();
        for (double lat = latIni; lat <= latMax; lat += paso) {
            for (double lon = lonIni; lon <= lonMax; lon += paso) {
                // Redondear a 4 decimales para evitar floating point drift
                double latR = Math.round(lat * 10000.0) / 10000.0;
                double lonR = Math.round(lon * 10000.0) / 10000.0;
                puntos.add(new PuntoMalla(latR, lonR, null));
            }
        }
        return puntos;
    }

    // -------------------------------------------------------
    // Helpers
    // -------------------------------------------------------

    /**
     * Calcula el paso de malla para no superar ~200 puntos en el bbox.
     * Usa el paso base (0.75°) para zooms altos, y lo aumenta para zoom out.
     */
    private double calcularPasoAdaptativo(double deltaLat, double deltaLon) {
        double puntosConPasoBase = (deltaLat / pasoMalla) * (deltaLon / pasoMalla);
        if (puntosConPasoBase <= 200) return pasoMalla;

        // Aumentar paso hasta que quepan ~200 puntos
        double factor = Math.ceil(Math.sqrt(puntosConPasoBase / 200.0));
        return Math.round(pasoMalla * factor * 100.0) / 100.0;
    }

    /**
     * Redondea una coordenada al múltiplo más cercano del paso de malla.
     * Así bboxes casi idénticos producen la misma clave de cache.
     * Ej: latMin=40.123 → 40.0 (con pasoMalla=0.75, múltiplo más cercano)
     */
    private double redondearBbox(double coord) {
        return Math.round(coord / pasoMalla) * pasoMalla;
    }

    /**
     * Comprueba si el bbox cae completamente dentro de España.
     * Usamos un margen de 1° para absorber coordenadas en los bordes.
     */
    private boolean esDentroEspana(double latMin, double latMax,
                                    double lonMin, double lonMax) {
        return latMin >= Constantes.ESP_LAT_MIN - 1 && latMax <= Constantes.ESP_LAT_MAX + 1
            && lonMin >= Constantes.ESP_LON_MIN - 1 && lonMax <= Constantes.ESP_LON_MAX + 1;
    }

    private <T> List<List<T>> dividirEnBatches(List<T> lista, int tamano) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < lista.size(); i += tamano) {
            batches.add(lista.subList(i, Math.min(i + tamano, lista.size())));
        }
        return batches;
    }
}
