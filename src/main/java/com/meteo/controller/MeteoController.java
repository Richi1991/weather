package com.meteo.controller;

import com.meteo.dto.CurrentGridDTO;
import com.meteo.repository.RedisRasterRepository;
import com.meteo.service.MeteoBboxService;
import com.meteo.service.MeteoQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/meteo")
@CrossOrigin(origins = "*")
public class MeteoController {

    @Autowired
    private MeteoQueryService queryService;

    @Autowired
    private MeteoBboxService bboxService;

    @Autowired
    private RedisRasterRepository redisRasterRepository;

    // -------------------------------------------------------
    // MAPA MUNDIAL (heatmap al desplazar)
    // -------------------------------------------------------

    @GetMapping("/raster/temperatura")
    public ResponseEntity<byte[]> getRasterTemperatura() {
        byte[] png = redisRasterRepository.getRasterTemperatura();

        if (png == null) {
            return ResponseEntity.noContent().build();
        }

        return ResponseEntity.ok()
                .header("Content-Type", "image/png")
                .header("Cache-Control", "max-age=3600")
                .body(png);
    }

    /**
     * Heatmap de precipitación para el bbox visible en el mapa.
     * Llamado desde el frontend en cada moveend (con debounce).
     *
     * - Si el bbox es España → sirve desde BD (rápido).
     * - Si es el resto del mundo → proxy a Open-Meteo con cache 1h.
     *
     * GET /api/meteo/grid/bbox?latMin=35&latMax=44&lonMin=-10&lonMax=5
     */
    @GetMapping("/grid/bbox")
    public ResponseEntity<List<Map<String, Object>>> getGridBbox(
            @RequestParam double latMin,
            @RequestParam double latMax,
            @RequestParam double lonMin,
            @RequestParam double lonMax) {

        // Validación básica de coordenadas
        if (latMin >= latMax || lonMin >= lonMax
                || latMin < -90 || latMax > 90
                || lonMin < -180 || lonMax > 180) {
            return ResponseEntity.badRequest().build();
        }

        return ResponseEntity.ok(
                bboxService.getGridPrecipitacionBbox(latMin, latMax, lonMin, lonMax));
    }

    /**
     * Heatmap de precipitación para el bbox visible en el mapa.
     * Llamado desde el frontend en cada moveend (con debounce).
     *
     * - Si el bbox es España → sirve desde BD (rápido).
     * - Si es el resto del mundo → proxy a Open-Meteo con cache 1h.
     *
     * GET /api/meteo/grid/bbox?latMin=35&latMax=44&lonMin=-10&lonMax=5
     */
    @GetMapping("/grid/current/bbox")
    public ResponseEntity<List<CurrentGridDTO>> getGridCurrentBbox(
            @RequestParam double latMin,
            @RequestParam double latMax,
            @RequestParam double lonMin,
            @RequestParam double lonMax) {

        // Validación básica de coordenadas
        if (latMin >= latMax || lonMin >= lonMax
                || latMin < -90 || latMax > 90
                || lonMin < -180 || lonMax > 180) {
            return ResponseEntity.badRequest().build();
        }

        return ResponseEntity.ok(
                bboxService.getGridCurrentBbox(latMin, latMax, lonMin, lonMax));
    }

    // -------------------------------------------------------
    // ENDPOINTS ORIGINALES (sin cambios)
    // -------------------------------------------------------

    /**
     * Grid completo de precipitación para el mapa heatmap de España.
     * GET /api/meteo/grid/precipitacion?fecha=2026-03-30
     */
    @GetMapping("/grid/precipitacion")
    public ResponseEntity<List<Map<String, Object>>> getGridPrecipitacion(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fecha) {
        return ResponseEntity.ok(bboxService.getGridEspanaCacheado(fecha));
    }

    /**
     * Grid completo con todas las variables para un día.
     * GET /api/meteo/grid/forecast?fecha=2026-03-30
     */
    @GetMapping("/grid/forecast")
    public ResponseEntity<List<Map<String, Object>>> getGridForecast(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fecha) {
        return ResponseEntity.ok(queryService.getGridForecast(fecha));
    }

    /**
     * Forecast de los próximos 15 días para un punto concreto.
     * GET /api/meteo/forecast/{lat}/{lon}
     */
    @GetMapping("/forecast/{lat}/{lon}")
    public ResponseEntity<List<Map<String, Object>>> getForecastPunto(
            @PathVariable double lat,
            @PathVariable double lon) {
        return ResponseEntity.ok(queryService.getForecastPunto(lat, lon));
    }

    /**
     * Datos actuales (última hora) para un punto.
     * GET /api/meteo/current/{lat}/{lon}
     */
    @GetMapping("/current/{lat}/{lon}")
    public ResponseEntity<Map<String, Object>> getCurrentPunto(
            @PathVariable double lat,
            @PathVariable double lon) {
        return ResponseEntity.ok(queryService.getCurrentPunto(lat, lon));
    }

    /**
     * Histórico de un punto entre dos fechas.
     * GET /api/meteo/historico/{lat}/{lon}?desde=2026-01-01&hasta=2026-03-29
     */
    @GetMapping("/historico/{lat}/{lon}")
    public ResponseEntity<List<Map<String, Object>>> getHistorico(
            @PathVariable double lat,
            @PathVariable double lon,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate desde,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate hasta) {
        return ResponseEntity.ok(queryService.getHistorico(lat, lon, desde, hasta));
    }

    /**
     * Grid actual (última lectura de cada punto) para el mapa en tiempo real.
     * GET /api/meteo/grid/current
     */
    @GetMapping("/grid/current")
    public ResponseEntity<List<Map<String, Object>>> getGridCurrent() {
        return ResponseEntity.ok(queryService.getGridCurrent());
    }

    /**
     * Estado de los últimos jobs ejecutados.
     * GET /api/meteo/jobs/status
     */
    @GetMapping("/jobs/status")
    public ResponseEntity<List<Map<String, Object>>> getJobsStatus() {
        return ResponseEntity.ok(queryService.getJobsStatus());
    }
}
