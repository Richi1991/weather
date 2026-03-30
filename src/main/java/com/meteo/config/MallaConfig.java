package com.meteo.config;

import com.meteo.model.PuntoMalla;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class MallaConfig {

    @Value("${meteo.openmeteo.paso-malla}")
    private double pasoMalla;

    @Bean
    public List<PuntoMalla> mallaEspana() {
        List<PuntoMalla> puntos = new ArrayList<>();

        // Península e islas Baleares
        for (double lat = 35.75; lat <= 43.75; lat += pasoMalla) {
            for (double lon = -9.25; lon <= 4.25; lon += pasoMalla) {
                puntos.add(new PuntoMalla(
                        Math.round(lat * 10000.0) / 10000.0,
                        Math.round(lon * 10000.0) / 10000.0,
                        "peninsula"
                ));
            }
        }

        // Canarias
        for (double lat = 27.5; lat <= 29.5; lat += pasoMalla) {
            for (double lon = -18.25; lon <= -13.25; lon += pasoMalla) {
                puntos.add(new PuntoMalla(
                        Math.round(lat * 10000.0) / 10000.0,
                        Math.round(lon * 10000.0) / 10000.0,
                        "canarias"
                ));
            }
        }

        return puntos;
    }
}
