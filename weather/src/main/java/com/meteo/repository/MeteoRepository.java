package com.meteo.repository;

import com.meteo.model.OpenMeteoResponse;
import com.meteo.model.PuntoMalla;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Repository
public class MeteoRepository {

    private static final Logger log = LoggerFactory.getLogger(MeteoRepository.class);
    private final DSLContext dsl;

    public MeteoRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    // -------------------------------------------------------
    // UPSERT FORECAST DIARIO
    // -------------------------------------------------------
    public void upsertForecast(PuntoMalla punto, OpenMeteoResponse response) {
        if (response == null || response.daily() == null) return;

        Integer puntoId = obtenerOCrearPunto(punto);
        var daily = response.daily();

        for (int i = 0; i < daily.time().size(); i++) {
            final int idx = i;
            try {
                LocalDate fecha = LocalDate.parse(daily.time().get(idx));

                dsl.execute("""
                    INSERT INTO meteo.forecast_diario (
                        punto_id, fecha,
                        temperature_max, temperature_min,
                        apparent_temperature_max, apparent_temperature_min,
                        precipitation_sum, rain_sum, showers_sum, snowfall_sum, precipitation_hours,
                        windspeed_max, windgusts_max, winddirection_dominant,
                        uv_index_max, sunshine_duration, shortwave_radiation_sum,
                        sunrise, sunset, weathercode, et0_fao_evapotranspiration,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::timestamptz, ?::timestamptz, ?, ?, NOW())
                    ON CONFLICT (punto_id, fecha) DO UPDATE SET
                        temperature_max               = EXCLUDED.temperature_max,
                        temperature_min               = EXCLUDED.temperature_min,
                        apparent_temperature_max      = EXCLUDED.apparent_temperature_max,
                        apparent_temperature_min      = EXCLUDED.apparent_temperature_min,
                        precipitation_sum             = EXCLUDED.precipitation_sum,
                        rain_sum                      = EXCLUDED.rain_sum,
                        showers_sum                   = EXCLUDED.showers_sum,
                        snowfall_sum                  = EXCLUDED.snowfall_sum,
                        precipitation_hours           = EXCLUDED.precipitation_hours,
                        windspeed_max                 = EXCLUDED.windspeed_max,
                        windgusts_max                 = EXCLUDED.windgusts_max,
                        winddirection_dominant        = EXCLUDED.winddirection_dominant,
                        uv_index_max                  = EXCLUDED.uv_index_max,
                        sunshine_duration             = EXCLUDED.sunshine_duration,
                        shortwave_radiation_sum       = EXCLUDED.shortwave_radiation_sum,
                        sunrise                       = EXCLUDED.sunrise,
                        sunset                        = EXCLUDED.sunset,
                        weathercode                   = EXCLUDED.weathercode,
                        et0_fao_evapotranspiration    = EXCLUDED.et0_fao_evapotranspiration,
                        updated_at                    = NOW()
                    """,
                        puntoId, fecha,
                        safeGet(daily.temperatureMax(), idx),
                        safeGet(daily.temperatureMin(), idx),
                        safeGet(daily.apparentTempMax(), idx),
                        safeGet(daily.apparentTempMin(), idx),
                        safeGet(daily.precipitationSum(), idx),
                        safeGet(daily.rainSum(), idx),
                        safeGet(daily.showersSum(), idx),
                        safeGet(daily.snowfallSum(), idx),
                        safeGet(daily.precipitationHours(), idx),
                        safeGet(daily.windspeedMax(), idx),
                        safeGet(daily.windgustsMax(), idx),
                        safeGet(daily.winddirectionDominant(), idx),
                        safeGet(daily.uvIndexMax(), idx),
                        safeGet(daily.sunshineDuration(), idx),
                        safeGet(daily.shortwaveRadiationSum(), idx),
                        DSL.val(parseTimestamp(safeGetStr(daily.sunrise(), idx)), SQLDataType.TIMESTAMPWITHTIMEZONE),
                        DSL.val(parseTimestamp(safeGetStr(daily.sunset(), idx)), SQLDataType.TIMESTAMPWITHTIMEZONE),
                        safeGetInt(daily.weathercode(), idx),
                        safeGet(daily.evapotranspiration(), idx)
                );
            } catch (Exception e) {
                log.warn("Error insertando forecast día {}: {}", i, e.getMessage());
            }
        }
    }

    // -------------------------------------------------------
    // INSERT CURRENT
    // -------------------------------------------------------
    public void insertCurrent(PuntoMalla punto, OpenMeteoResponse response) {
        if (response == null || response.current() == null) return;

        Integer puntoId = obtenerOCrearPunto(punto);
        var c = response.current();

        try {
            dsl.execute("""
                INSERT INTO meteo.current (
                    punto_id, timestamp,
                    temperature_2m, apparent_temperature,
                    relative_humidity_2m, dewpoint_2m,
                    precipitation, rain, snowfall,
                    windspeed_10m, windgusts_10m, winddirection_10m,
                    surface_pressure, cloudcover, visibility,
                    shortwave_radiation, uv_index, weathercode, is_day
                ) VALUES (?, ?::timestamptz, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (punto_id, timestamp) DO NOTHING
                """,
                    puntoId, parseTimestamp(c.time()),
                    c.temperature2m(), c.apparentTemperature(),
                    c.relativeHumidity(), c.dewpoint(),
                    c.precipitation(), c.rain(), c.snowfall(),
                    c.windspeed(), c.windgusts(), c.winddirection(),
                    c.surfacePressure(), c.cloudcover(), c.visibility(),
                    c.shortwaveRadiation(), c.uvIndex(), c.weathercode(),
                    c.isDay() != null && c.isDay() == 1
            );
        } catch (Exception e) {
            log.warn("Error insertando current para punto ({},{}): {}",
                    punto.getLatitud(), punto.getLongitud(), e.getMessage());
        }
    }

    // -------------------------------------------------------
    // UPSERT HISTÓRICO
    // -------------------------------------------------------
    public void upsertHistorico(PuntoMalla punto, OpenMeteoResponse response) {
        if (response == null || response.daily() == null) return;

        Integer puntoId = obtenerOCrearPunto(punto);
        var daily = response.daily();

        for (int i = 0; i < daily.time().size(); i++) {
            final int idx = i;
            try {
                LocalDate fecha = LocalDate.parse(daily.time().get(idx));

                dsl.execute("""
                    INSERT INTO meteo.historico (
                        punto_id, fecha,
                        temperature_max, temperature_min,
                        precipitation_sum, rain_sum, snowfall_sum,
                        windspeed_max, windgusts_max,
                        sunshine_duration, shortwave_radiation_sum,
                        et0_fao_evapotranspiration, weathercode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (punto_id, fecha) DO NOTHING
                    """,
                        puntoId, fecha,
                        safeGet(daily.temperatureMax(), idx),
                        safeGet(daily.temperatureMin(), idx),
                        safeGet(daily.precipitationSum(), idx),
                        safeGet(daily.rainSum(), idx),
                        safeGet(daily.snowfallSum(), idx),
                        safeGet(daily.windspeedMax(), idx),
                        safeGet(daily.windgustsMax(), idx),
                        safeGet(daily.sunshineDuration(), idx),
                        safeGet(daily.shortwaveRadiationSum(), idx),
                        safeGet(daily.evapotranspiration(), idx),
                        safeGetInt(daily.weathercode(), idx)
                );
            } catch (Exception e) {
                log.warn("Error insertando histórico día {}: {}", i, e.getMessage());
            }
        }
    }

    // -------------------------------------------------------
    // LOG DE JOB
    // -------------------------------------------------------
    public void logJob(String nombre, String estado, int total, int ok, int error,
                       long duracionMs, String mensajeError) {
        dsl.execute("""
            INSERT INTO meteo.job_log (
                job_nombre, estado, puntos_total, puntos_ok, puntos_error, duracion_ms, mensaje_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, nombre, estado, total, ok, error, duracionMs, mensajeError);
    }

    // -------------------------------------------------------
    // OBTENER O CREAR PUNTO EN LA MALLA
    // -------------------------------------------------------
    private Integer obtenerOCrearPunto(PuntoMalla punto) {
        var result = dsl.fetch(
                "SELECT id FROM meteo.grid_puntos WHERE latitud = ? AND longitud = ?",
                punto.getLatitud(), punto.getLongitud()
        );

        if (!result.isEmpty()) {
            return result.get(0).get("id", Integer.class);
        }

        return dsl.fetchOne("""
                INSERT INTO meteo.grid_puntos (latitud, longitud, region)
                VALUES (?, ?, ?) RETURNING id
                """, punto.getLatitud(), punto.getLongitud(), punto.getRegion())
                .get("id", Integer.class);
    }

    // -------------------------------------------------------
    // Helpers null-safe
    // -------------------------------------------------------
    private <T> T safeGet(java.util.List<T> list, int idx) {
        if (list == null || idx >= list.size()) return null;
        return list.get(idx);
    }

    private String safeGetStr(java.util.List<String> list, int idx) {
        if (list == null || idx >= list.size()) return null;
        return list.get(idx);
    }

    private Integer safeGetInt(java.util.List<Integer> list, int idx) {
        if (list == null || idx >= list.size()) return null;
        return list.get(idx);
    }

    private OffsetDateTime parseTimestamp(String ts) {
        if (ts == null) return null;
        try {
            return LocalDateTime
                    .parse(ts, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"))
                    .atOffset(ZoneOffset.UTC);
        } catch (Exception e) {
            return null;
        }
    }
}