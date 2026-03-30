package com.meteo.model;

/**
 * Representa un punto de la malla geográfica.
 * Usado para construir las llamadas batch a Open-Meteo
 * y para poblar la tabla meteo.grid_puntos.
 */
public class PuntoMalla {

    private final double latitud;
    private final double longitud;
    private final String region;  // 'peninsula', 'canarias', 'baleares', 'ceuta', 'melilla' — null para puntos mundiales

    public PuntoMalla(double latitud, double longitud, String region) {
        this.latitud  = latitud;
        this.longitud = longitud;
        this.region   = region;
    }

    public double getLatitud()  { return latitud;  }
    public double getLongitud() { return longitud; }
    public String getRegion()   { return region;   }

    @Override
    public String toString() {
        return "PuntoMalla{lat=" + latitud + ", lon=" + longitud + ", region=" + region + "}";
    }
}
