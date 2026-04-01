package com.meteo.service;

import com.meteo.model.PuntoTemperatura;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class InterpolacionService {

    private static final double LAT_MIN  = 27.0;
    private static final double LAT_MAX  = 44.0;
    private static final double LON_MIN  = -18.5;
    private static final double LON_MAX  =  4.5;
    private static final double PASO     = 0.05;
    private static final int    VECINOS  = 4;
    private static final double POTENCIA = 2.0;

    /**
     * Interpola los puntos reales a un grid regular de 0.05°
     * usando IDW (Inverse Distance Weighting)
     */
    public List<PuntoTemperatura> interpolarGrid(List<PuntoTemperatura> puntosReales) {
        List<PuntoTemperatura> grid = new ArrayList<>();

        for (double lat = LAT_MIN; lat <= LAT_MAX; lat += PASO) {
            for (double lon = LON_MIN; lon <= LON_MAX; lon += PASO) {
                double latR = Math.round(lat * 1000.0) / 1000.0;
                double lonR = Math.round(lon * 1000.0) / 1000.0;

                double temp = idw(puntosReales, latR, lonR);
                grid.add(new PuntoTemperatura(latR, lonR, temp));
            }
        }

        return grid;
    }

    private double idw(List<PuntoTemperatura> puntos, double lat, double lon) {
        // Calcular distancias
        record Distancia(PuntoTemperatura punto, double dist) {}

        List<Distancia> distancias = puntos.stream()
                .map(p -> new Distancia(p, Math.sqrt(
                        Math.pow(p.latitud() - lat, 2) +
                                Math.pow(p.longitud() - lon, 2)
                )))
                .sorted((a, b) -> Double.compare(a.dist(), b.dist()))
                .limit(VECINOS)
                .toList();

        // Punto exacto
        if (!distancias.isEmpty() && distancias.get(0).dist() < 0.001) {
            return distancias.get(0).punto().temperatura();
        }

        // IDW
        double sumPesos  = 0;
        double sumValores = 0;

        for (var d : distancias) {
            double peso = 1.0 / Math.pow(d.dist(), POTENCIA);
            sumPesos   += peso;
            sumValores += peso * d.punto().temperatura();
        }

        return sumPesos > 0 ? sumValores / sumPesos : 15.0;
    }
}