package com.meteo.model;

public record PuntoTemperatura(
        double latitud,
        double longitud,
        double temperatura
) {}