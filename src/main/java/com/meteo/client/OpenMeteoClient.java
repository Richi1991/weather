package com.meteo.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meteo.model.OpenMeteoResponse;
import com.meteo.model.PuntoMalla;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class OpenMeteoClient {

    private static final Logger log = LoggerFactory.getLogger(OpenMeteoClient.class);

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .version(HttpClient.Version.HTTP_1_1)  // ← forzar HTTP/1.1
            .build();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Variables diarias para forecast
    private static final String DAILY_VARS =
            "temperature_2m_max,temperature_2m_min," +
            "apparent_temperature_max,apparent_temperature_min," +
            "precipitation_sum,rain_sum,showers_sum,snowfall_sum,precipitation_hours," +
            "windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant," +
            "uv_index_max,sunshine_duration,shortwave_radiation_sum," +
            "sunrise,sunset,weathercode,et0_fao_evapotranspiration";

    // Variables actuales (current)
    private static final String CURRENT_VARS =
            "temperature_2m,apparent_temperature,relative_humidity_2m,dewpoint_2m," +
            "precipitation,rain,snowfall," +
            "windspeed_10m,windgusts_10m,winddirection_10m," +
            "surface_pressure,cloudcover,visibility," +
            "shortwave_radiation,uv_index,weathercode,is_day";

    @Value("${meteo.openmeteo.base-url}")
    private String baseUrl;

    @Value("${meteo.openmeteo.base-url-historico}")
    private String baseUrlHistorico;

    @Value("${meteo.openmeteo.forecast-days}")
    private int forecastDays;

    /**
     * Consulta forecast diario para un punto individual.
     * Usado para puntos específicos (ej: estaciones meteorológicas propias).
     */
    public CompletableFuture<OpenMeteoResponse> getForecastPunto(double lat, double lon) {
        String url = baseUrl + "/forecast"
                + "?latitude=" + lat
                + "&longitude=" + lon
                + "&daily=" + DAILY_VARS
                + "&current=" + CURRENT_VARS
                + "&timezone=Europe/Madrid"
                + "&forecast_days=" + forecastDays;

        return fetchAsync(url, OpenMeteoResponse.class);
    }

    /**
     * Consulta forecast en batch para múltiples puntos de la malla.
     * Open-Meteo devuelve un array cuando se pasan múltiples coordenadas.
     * Limitamos a 50 puntos por llamada para evitar URLs demasiado largas.
     */
    public CompletableFuture<OpenMeteoResponse[]> getForecastBatch(List<PuntoMalla> puntos) {
        String lats = puntos.stream()
                .map(p -> String.valueOf(p.getLatitud()))
                .reduce((a, b) -> a + "," + b).orElse("");

        String lons = puntos.stream()
                .map(p -> String.valueOf(p.getLongitud()))
                .reduce((a, b) -> a + "," + b).orElse("");

        String url = baseUrl + "/forecast"
                + "?latitude=" + lats
                + "&longitude=" + lons
                + "&daily=" + DAILY_VARS
                + "&timezone=Europe/Madrid"
                + "&forecast_days=" + forecastDays;

        return fetchAsync(url, OpenMeteoResponse[].class);
    }

    /**
     * Consulta datos actuales (current) en batch.
     */
    public CompletableFuture<OpenMeteoResponse[]> getCurrentBatch(List<PuntoMalla> puntos) {
        String lats = puntos.stream()
                .map(p -> String.valueOf(p.getLatitud()))
                .reduce((a, b) -> a + "," + b).orElse("");

        String lons = puntos.stream()
                .map(p -> String.valueOf(p.getLongitud()))
                .reduce((a, b) -> a + "," + b).orElse("");

        String url = baseUrl + "/forecast"
                + "?latitude=" + lats
                + "&longitude=" + lons
                + "&current=" + CURRENT_VARS
                + "&timezone=Europe/Madrid";

        return fetchAsync(url, OpenMeteoResponse[].class);
    }

    /**
     * Consulta datos históricos (archive API).
     */
    public CompletableFuture<OpenMeteoResponse[]> getHistoricoBatch(
            List<PuntoMalla> puntos, String fechaInicio, String fechaFin) {

        String lats = puntos.stream()
                .map(p -> String.valueOf(p.getLatitud()))
                .reduce((a, b) -> a + "," + b).orElse("");

        String lons = puntos.stream()
                .map(p -> String.valueOf(p.getLongitud()))
                .reduce((a, b) -> a + "," + b).orElse("");

        String historicalVars =
                "temperature_2m_max,temperature_2m_min,temperature_2m_mean," +
                "precipitation_sum,rain_sum,snowfall_sum," +
                "windspeed_10m_max,windgusts_10m_max," +
                "sunshine_duration,shortwave_radiation_sum," +
                "et0_fao_evapotranspiration,weathercode";

        String url = baseUrlHistorico + "/archive"  // endpoint histórico de Open-Meteo
                + "?latitude=" + lats
                + "&longitude=" + lons
                + "&daily=" + historicalVars
                + "&timezone=Europe/Madrid"
                + "&start_date=" + fechaInicio
                + "&end_date=" + fechaFin;

        return fetchAsync(url, OpenMeteoResponse[].class);
    }

    private <T> CompletableFuture<T> fetchAsync(String url, Class<T> responseType) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(60))
                .build();

        return HTTP_CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() != 200) {
                        log.error("Open-Meteo error {} para URL: {}", response.statusCode(), url);
                        throw new RuntimeException("Open-Meteo HTTP " + response.statusCode());
                    }
                    try {
                        return MAPPER.readValue(response.body(), responseType);
                    } catch (Exception e) {
                        throw new RuntimeException("Error parseando respuesta Open-Meteo", e);
                    }
                });
    }
}
