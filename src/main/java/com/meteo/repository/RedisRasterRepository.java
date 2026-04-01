package com.meteo.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.Base64;

@Repository
public class RedisRasterRepository {

    private static final Logger log = LoggerFactory.getLogger(RedisRasterRepository.class);
    private static final String KEY_TEMPERATURA = "raster:temperatura";
    private static final Duration TTL = Duration.ofHours(2);

    private final RedisTemplate<String, Object> redisTemplate;

    public RedisRasterRepository(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void saveRasterTemperatura(byte[] png) {
        try {
            String base64 = Base64.getEncoder().encodeToString(png);
            redisTemplate.opsForValue().set(KEY_TEMPERATURA, base64, TTL);
            log.info("Raster temperatura guardado en Redis — {} bytes", png.length);
        } catch (Exception e) {
            log.error("Error guardando raster en Redis: {}", e.getMessage());
        }
    }

    public byte[] getRasterTemperatura() {
        try {
            Object value = redisTemplate.opsForValue().get(KEY_TEMPERATURA);
            if (value == null) return null;
            return Base64.getDecoder().decode((String) value);
        } catch (Exception e) {
            log.error("Error leyendo raster de Redis: {}", e.getMessage());
            return null;
        }
    }
}