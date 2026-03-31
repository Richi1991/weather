package com.meteo.common;

import java.time.Duration;

public class Constantes {

    // Máx puntos por batch para no superar límite de URL (~2000 chars)
    public static final int BATCH_SIZE = 100;
    public static final int CHUNK_SIZE = 10;

    // Límites geográficos de España (península + islas)
    public static final double ESP_LAT_MIN =  27.5;
    public static final double ESP_LAT_MAX =  43.8;
    public static final double ESP_LON_MIN = -18.2;
    public static final double ESP_LON_MAX =   4.4;
    public static final String PREFIX = "current:";
    public static final Duration TTL = Duration.ofHours(2);
}


