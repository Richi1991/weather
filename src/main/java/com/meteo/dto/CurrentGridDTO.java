package com.meteo.dto;

public record CurrentGridDTO(
        double latitud,
        double longitud,
        String timestamp,
        double temperature2m,
        double apparentTemperature,
        int relativeHumidity2m,
        double dewpoint2m,
        double precipitation,
        double rain,
        double snowfall,
        double windspeed10m,
        double windgusts10m,
        double winddirection10m,
        double surfacePressure,
        int cloudcover,
        double visibility,
        double shortwaveRadiation,
        double uvIndex,
        int weathercode,
        boolean isDay
) {}
