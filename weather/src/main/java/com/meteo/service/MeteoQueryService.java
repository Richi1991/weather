package com.meteo.service;

import org.jooq.DSLContext;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Service
public class MeteoQueryService {

    private final DSLContext dsl;

    public MeteoQueryService(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Grid de precipitación para heatmap (solo lat, lon, precipitacion).
     * Respuesta ligera optimizada para el frontend.
     */
    public List<Map<String, Object>> getGridPrecipitacion(LocalDate fecha) {
        return dsl.fetch("""
            SELECT p.latitud, p.longitud, f.precipitation_sum
            FROM meteo.forecast_diario f
            JOIN meteo.grid_puntos p ON p.id = f.punto_id
            WHERE f.fecha = ?
            ORDER BY p.latitud, p.longitud
            """, fecha)
                .intoMaps();
    }

    /**
     * Grid de precipitación filtrado por bbox geográfico.
     * Usado por MeteoBboxService cuando el mapa está centrado en España.
     * Solo devuelve los puntos visibles, no toda la malla.
     */
    public List<Map<String, Object>> getGridPrecipitacionBbox(
            double latMin, double latMax, double lonMin, double lonMax) {
        return dsl.fetch("""
            SELECT p.latitud, p.longitud, f.precipitation_sum
            FROM meteo.forecast_diario f
            JOIN meteo.grid_puntos p ON p.id = f.punto_id
            WHERE f.fecha = CURRENT_DATE
              AND p.latitud  BETWEEN ? AND ?
              AND p.longitud BETWEEN ? AND ?
            ORDER BY p.latitud, p.longitud
            """, latMin, latMax, lonMin, lonMax)
                .intoMaps();
    }

    /**
     * Grid completo con todas las variables para un día.
     */
    public List<Map<String, Object>> getGridForecast(LocalDate fecha) {
        return dsl.fetch("""
            SELECT
                p.latitud, p.longitud, p.region,
                f.fecha,
                f.temperature_max, f.temperature_min,
                f.apparent_temperature_max, f.apparent_temperature_min,
                f.precipitation_sum, f.rain_sum, f.snowfall_sum, f.precipitation_hours,
                f.windspeed_max, f.windgusts_max, f.winddirection_dominant,
                f.uv_index_max, f.sunshine_duration,
                f.sunrise, f.sunset,
                f.weathercode
            FROM meteo.forecast_diario f
            JOIN meteo.grid_puntos p ON p.id = f.punto_id
            WHERE f.fecha = ?
            ORDER BY p.latitud, p.longitud
            """, fecha)
                .intoMaps();
    }

    /**
     * Forecast 15 días para el punto más cercano a las coordenadas dadas.
     */
    public List<Map<String, Object>> getForecastPunto(double lat, double lon) {
        return dsl.fetch("""
            SELECT
                f.fecha,
                f.temperature_max, f.temperature_min,
                f.apparent_temperature_max, f.apparent_temperature_min,
                f.precipitation_sum, f.rain_sum, f.snowfall_sum,
                f.windspeed_max, f.windgusts_max, f.winddirection_dominant,
                f.uv_index_max, f.sunshine_duration,
                f.sunrise, f.sunset, f.weathercode,
                f.shortwave_radiation_sum, f.et0_fao_evapotranspiration
            FROM meteo.forecast_diario f
            JOIN meteo.grid_puntos p ON p.id = f.punto_id
            WHERE f.fecha >= CURRENT_DATE
            ORDER BY
                (p.latitud - ?)^2 + (p.longitud - ?)^2 ASC,
                f.fecha ASC
            LIMIT 15
            """, lat, lon)
                .intoMaps();
    }

    /**
     * Dato actual más reciente del punto más cercano.
     */
    public Map<String, Object> getCurrentPunto(double lat, double lon) {
        var result = dsl.fetch("""
            SELECT
                c.timestamp,
                c.temperature_2m, c.apparent_temperature,
                c.relative_humidity_2m, c.dewpoint_2m,
                c.precipitation, c.rain, c.snowfall,
                c.windspeed_10m, c.windgusts_10m, c.winddirection_10m,
                c.surface_pressure, c.cloudcover, c.visibility,
                c.uv_index, c.weathercode, c.is_day
            FROM meteo.current c
            JOIN meteo.grid_puntos p ON p.id = c.punto_id
            ORDER BY
                (p.latitud - ?)^2 + (p.longitud - ?)^2 ASC,
                c.timestamp DESC
            LIMIT 1
            """, lat, lon);

        return result.isEmpty() ? Map.of() : result.get(0).intoMap();
    }

    /**
     * Histórico de un punto entre dos fechas.
     */
    public List<Map<String, Object>> getHistorico(double lat, double lon,
                                                   LocalDate desde, LocalDate hasta) {
        return dsl.fetch("""
            SELECT
                h.fecha,
                h.temperature_max, h.temperature_min, h.temperature_mean,
                h.precipitation_sum, h.rain_sum, h.snowfall_sum,
                h.windspeed_max, h.windgusts_max,
                h.sunshine_duration, h.shortwave_radiation_sum,
                h.et0_fao_evapotranspiration, h.weathercode
            FROM meteo.historico h
            JOIN meteo.grid_puntos p ON p.id = h.punto_id
            WHERE h.fecha BETWEEN ? AND ?
            ORDER BY
                (p.latitud - ?)^2 + (p.longitud - ?)^2 ASC,
                h.fecha ASC
            """, desde, hasta, lat, lon)
                .intoMaps();
    }

    /**
     * Grid con la última lectura actual de cada punto (para mapa tiempo real).
     */
    public List<Map<String, Object>> getGridCurrent() {
        return dsl.fetch("""
            SELECT DISTINCT ON (c.punto_id)
                p.latitud, p.longitud,
                c.timestamp,
                c.temperature_2m,
                c.precipitation,
                c.windspeed_10m,
                c.weathercode,
                c.is_day
            FROM meteo.current c
            JOIN meteo.grid_puntos p ON p.id = c.punto_id
            ORDER BY c.punto_id, c.timestamp DESC
            """)
                .intoMaps();
    }

    /**
     * Estado del último job de cada tipo.
     */
    public List<Map<String, Object>> getJobsStatus() {
        return dsl.fetch("""
            SELECT DISTINCT ON (job_nombre)
                job_nombre, estado, puntos_total, puntos_ok, puntos_error,
                duracion_ms, mensaje_error, ejecutado_at
            FROM meteo.job_log
            ORDER BY job_nombre, ejecutado_at DESC
            """)
                .intoMaps();
    }
}
