package com.meteo.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Mapea la respuesta JSON de Open-Meteo.
 *
 * Open-Meteo devuelve esta estructura tanto para forecast como para archive:
 *
 * {
 *   "latitude": 40.0,
 *   "longitude": -3.75,
 *   "current": { "time": "2026-03-29T12:00", "temperature_2m": 18.5, ... },
 *   "daily": {
 *     "time": ["2026-03-29", "2026-03-30", ...],
 *     "temperature_2m_max": [22.1, 19.3, ...],
 *     ...
 *   }
 * }
 *
 * Cuando se hace una llamada batch (múltiples lat/lon), la API devuelve un array
 * de estos objetos: OpenMeteoResponse[]
 *
 * @JsonIgnoreProperties(ignoreUnknown = true) permite que si Open-Meteo añade
 * nuevos campos en el futuro, Jackson no rompa el parseo.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OpenMeteoResponse(

        double latitude,
        double longitude,

        @JsonProperty("current")
        Current current,

        @JsonProperty("daily")
        Daily daily

) {

    // -------------------------------------------------------
    // Datos actuales (current weather)
    // -------------------------------------------------------
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Current(

            @JsonProperty("time")
            String time,

            @JsonProperty("temperature_2m")
            Double temperature2m,

            @JsonProperty("apparent_temperature")
            Double apparentTemperature,

            @JsonProperty("relative_humidity_2m")
            Integer relativeHumidity,

            @JsonProperty("dewpoint_2m")
            Double dewpoint,

            @JsonProperty("precipitation")
            Double precipitation,

            @JsonProperty("rain")
            Double rain,

            @JsonProperty("snowfall")
            Double snowfall,

            @JsonProperty("windspeed_10m")
            Double windspeed,

            @JsonProperty("windgusts_10m")
            Double windgusts,

            @JsonProperty("winddirection_10m")
            Double winddirection,

            @JsonProperty("surface_pressure")
            Double surfacePressure,

            @JsonProperty("cloudcover")
            Integer cloudcover,

            @JsonProperty("visibility")
            Double visibility,

            @JsonProperty("shortwave_radiation")
            Double shortwaveRadiation,

            @JsonProperty("uv_index")
            Double uvIndex,

            @JsonProperty("weathercode")
            Integer weathercode,

            @JsonProperty("is_day")
            Integer isDay
    ) {}

    // -------------------------------------------------------
    // Predicción / histórico diario
    // -------------------------------------------------------
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Daily(

            @JsonProperty("time")
            List<String> time,

            // Temperatura
            @JsonProperty("temperature_2m_max")
            List<Double> temperatureMax,

            @JsonProperty("temperature_2m_min")
            List<Double> temperatureMin,

            @JsonProperty("temperature_2m_mean")
            List<Double> temperatureMean,       // solo en histórico

            @JsonProperty("apparent_temperature_max")
            List<Double> apparentTempMax,

            @JsonProperty("apparent_temperature_min")
            List<Double> apparentTempMin,

            // Precipitación
            @JsonProperty("precipitation_sum")
            List<Double> precipitationSum,

            @JsonProperty("rain_sum")
            List<Double> rainSum,

            @JsonProperty("showers_sum")
            List<Double> showersSum,

            @JsonProperty("snowfall_sum")
            List<Double> snowfallSum,

            @JsonProperty("precipitation_hours")
            List<Double> precipitationHours,

            // Viento
            @JsonProperty("windspeed_10m_max")
            List<Double> windspeedMax,

            @JsonProperty("windgusts_10m_max")
            List<Double> windgustsMax,

            @JsonProperty("winddirection_10m_dominant")
            List<Double> winddirectionDominant,

            // Radiación / Sol
            @JsonProperty("uv_index_max")
            List<Double> uvIndexMax,

            @JsonProperty("sunshine_duration")
            List<Double> sunshineDuration,

            @JsonProperty("shortwave_radiation_sum")
            List<Double> shortwaveRadiationSum,

            // Amanecer / Atardecer (vienen como String "2026-03-29T07:23")
            @JsonProperty("sunrise")
            List<String> sunrise,

            @JsonProperty("sunset")
            List<String> sunset,

            // Estado del tiempo
            @JsonProperty("weathercode")
            List<Integer> weathercode,

            // Evapotranspiración
            @JsonProperty("et0_fao_evapotranspiration")
            List<Double> evapotranspiration

    ) {}
}
