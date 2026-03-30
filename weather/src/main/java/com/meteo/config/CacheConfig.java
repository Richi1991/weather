package com.meteo.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class CacheConfig {

    /**
     * Cache "bbox-mundial":
     *   - Guarda los resultados de Open-Meteo para zonas del mundo fuera de España.
     *   - Clave: "lat_min:lat_max:lon_min:lon_max" redondeado a 0.75°
     *   - Expira en 1 hora (los datos de Open-Meteo se actualizan cada hora).
     *   - Máximo 500 entradas (~500 bboxes distintas en memoria).
     *
     * Cache "grid-espana":
     *   - Guarda el grid completo de España para el heatmap inicial.
     *   - Expira en 6 horas (sincronizado con el job de forecast).
     *   - Solo 10 entradas (una por fecha).
     */
    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager manager = new CaffeineCacheManager();

        manager.registerCustomCache("bbox-mundial",
                Caffeine.newBuilder()
                        .expireAfterWrite(1, TimeUnit.HOURS)
                        .maximumSize(500)
                        .recordStats()  // permite ver hit rate en logs si quieres
                        .build());

        manager.registerCustomCache("grid-espana",
                Caffeine.newBuilder()
                        .expireAfterWrite(6, TimeUnit.HOURS)
                        .maximumSize(10)
                        .build());

        return manager;
    }
}
