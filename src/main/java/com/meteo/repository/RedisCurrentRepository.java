package com.meteo.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meteo.common.Constantes;
import com.meteo.dto.CurrentGridDTO;
import com.meteo.model.OpenMeteoResponse;
import com.meteo.model.PuntoMalla;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class RedisCurrentRepository {

    private static final Logger log = LoggerFactory.getLogger(RedisCurrentRepository.class);


    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // -------------------------------------------------------
    // GUARDAR current de un punto
    // -------------------------------------------------------
    public void saveCurrent(PuntoMalla punto, OpenMeteoResponse response) {
        if (response == null || response.current() == null) return;

        var c = response.current();
        String key = buildKey(punto.getLatitud(), punto.getLongitud());

        Map<String, Object> data = new HashMap<>();
        data.put("latitud",              punto.getLatitud());
        data.put("longitud",             punto.getLongitud());
        data.put("timestamp",            c.time());
        data.put("temperature_2m",       c.temperature2m());
        data.put("apparent_temperature", c.apparentTemperature());
        data.put("relative_humidity_2m", c.relativeHumidity());
        data.put("dewpoint_2m",          c.dewpoint());
        data.put("precipitation",        c.precipitation());
        data.put("rain",                 c.rain());
        data.put("snowfall",             c.snowfall());
        data.put("windspeed_10m",        c.windspeed());
        data.put("windgusts_10m",        c.windgusts());
        data.put("winddirection_10m",    c.winddirection());
        data.put("surface_pressure",     c.surfacePressure());
        data.put("cloudcover",           c.cloudcover());
        data.put("visibility",           c.visibility());
        data.put("shortwave_radiation",  c.shortwaveRadiation());
        data.put("uv_index",             c.uvIndex());
        data.put("weathercode",          c.weathercode());
        data.put("is_day",               c.isDay() != null && c.isDay() == 1);

        try {
            redisTemplate.opsForValue().set(key, data, Constantes.TTL);
        } catch (Exception e) {
            log.warn("Error guardando current en Redis para punto ({},{}): {}",
                    punto.getLatitud(), punto.getLongitud(), e.getMessage());
        }
    }

    // -------------------------------------------------------
    // OBTENER current de un punto concreto
    // -------------------------------------------------------
    @SuppressWarnings("unchecked")
    public Map<String, Object> getCurrent(double lat, double lon) {
        String key = buildKey(lat, lon);
        try {
            Object value = redisTemplate.opsForValue().get(key);
            if (value instanceof Map) return (Map<String, Object>) value;
        } catch (Exception e) {
            log.warn("Error leyendo current de Redis: {}", e.getMessage());
        }
        return Map.of();
    }

    // -------------------------------------------------------
    // OBTENER todos los current de España (para grid/current)
    // -------------------------------------------------------
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getAllCurrentEspana() {
        List<Map<String, Object>> resultado = new ArrayList<>();
        try {
            var keys = redisTemplate.keys(Constantes.PREFIX + "*");
            if (keys == null || keys.isEmpty()) return resultado;

            var values = redisTemplate.opsForValue().multiGet(keys);
            if (values == null) return resultado;

            for (Object value : values) {
                if (value instanceof Map) {
                    resultado.add((Map<String, Object>) value);
                }
            }
        } catch (Exception e) {
            log.warn("Error leyendo grid current de Redis: {}", e.getMessage());
        }
        return resultado;
    }

    // -------------------------------------------------------
    // OBTENER current por bbox
    // -------------------------------------------------------
    public List<CurrentGridDTO> getCurrentBbox(
            double latMin, double latMax, double lonMin, double lonMax) {

        return getAllCurrentEspana().stream()
                .filter(p -> {
                    double lat = ((Number) p.getOrDefault("latitud", 0.0)).doubleValue();
                    double lon = ((Number) p.getOrDefault("longitud", 0.0)).doubleValue();
                    return lat >= latMin && lat <= latMax
                            && lon >= lonMin && lon <= lonMax;
                })
                .map(p -> new CurrentGridDTO(
                        ((Number) p.getOrDefault("latitud", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("longitud", 0.0)).doubleValue(),
                        (String) p.getOrDefault("timestamp", ""),
                        ((Number) p.getOrDefault("temperature_2m", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("apparent_temperature", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("relative_humidity_2m", 0)).intValue(),
                        ((Number) p.getOrDefault("dewpoint_2m", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("precipitation", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("rain", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("snowfall", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("windspeed_10m", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("windgusts_10m", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("winddirection_10m", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("surface_pressure", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("cloudcover", 0)).intValue(),
                        ((Number) p.getOrDefault("visibility", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("shortwave_radiation", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("uv_index", 0.0)).doubleValue(),
                        ((Number) p.getOrDefault("weathercode", 0)).intValue(),
                        (Boolean) p.getOrDefault("is_day", false)
                ))
                .toList();
    }

    // -------------------------------------------------------
    // Helper
    // -------------------------------------------------------
    private String buildKey(double lat, double lon) {
        return Constantes.PREFIX + lat + ":" + lon;
    }
}