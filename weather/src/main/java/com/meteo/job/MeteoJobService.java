package com.meteo.job;

import com.meteo.client.OpenMeteoClient;
import com.meteo.config.MallaConfig;
import com.meteo.model.PuntoMalla;
import com.meteo.repository.MeteoRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class MeteoJobService {

    private static final Logger log = LoggerFactory.getLogger(MeteoJobService.class);

    // Máx puntos por batch para no superar límite de URL (~2000 chars)
    private static final int BATCH_SIZE = 50;
    private static final int CHUNK_SIZE = 10;

    private final OpenMeteoClient openMeteoClient;
    private final MeteoRepository meteoRepository;
    private final List<PuntoMalla> mallaEspana;

    @Autowired
    private MallaConfig mallaConfig;

    public MeteoJobService(OpenMeteoClient openMeteoClient,
                           MeteoRepository meteoRepository,
                           List<PuntoMalla> mallaEspana) {
        this.openMeteoClient = openMeteoClient;
        this.meteoRepository = meteoRepository;
        this.mallaEspana = mallaEspana;
    }

    // -------------------------------------------------------
    // JOB 1: FORECAST (cada 6 horas, sincronizado con Open-Meteo)
    // -------------------------------------------------------
    @Scheduled(cron = "${meteo.jobs.forecast-cron}")
    public void jobForecast() {
        log.info("=== JOB FORECAST iniciado. {} puntos totales ===", mallaEspana.size());
        long inicio = System.currentTimeMillis();
        int puntosOk = 0;
        int puntosError = 0;

        List<List<PuntoMalla>> batches = dividirEnBatches(mallaEspana, BATCH_SIZE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (List<PuntoMalla> batch : batches) {
            CompletableFuture<Void> future = openMeteoClient
                    .getForecastBatch(batch)
                    .thenAccept(responses -> {
                        for (int i = 0; i < responses.length; i++) {
                            meteoRepository.upsertForecast(batch.get(i), responses[i]);
                        }
                    })
                    .exceptionally(e -> {
                        log.error("Error en batch forecast: {}", e.getMessage());
                        return null;
                    });
            futures.add(future);
        }

        // Esperar a todos los batches
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long duracion = System.currentTimeMillis() - inicio;
        log.info("=== JOB FORECAST completado en {}ms ===", duracion);
        meteoRepository.logJob("FORECAST", "OK", mallaEspana.size(), puntosOk, puntosError, duracion, null);
    }

    // -------------------------------------------------------
    // JOB 2: DATOS ACTUALES (cada hora)
    // -------------------------------------------------------
    @Scheduled(cron = "${meteo.jobs.current-cron}")
    public void jobCurrent() {
        log.info("=== JOB CURRENT iniciado ===");
        long inicio = System.currentTimeMillis();

        List<List<PuntoMalla>> batches = dividirEnBatches(mallaEspana, BATCH_SIZE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (List<PuntoMalla> batch : batches) {
            CompletableFuture<Void> future = openMeteoClient
                    .getCurrentBatch(batch)
                    .thenAccept(responses -> {
                        for (int i = 0; i < responses.length; i++) {
                            meteoRepository.insertCurrent(batch.get(i), responses[i]);
                        }
                    })
                    .exceptionally(e -> {
                        log.error("Error en batch current: {}", e.getMessage());
                        return null;
                    });
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long duracion = System.currentTimeMillis() - inicio;
        log.info("=== JOB CURRENT completado en {}ms ===", duracion);
        meteoRepository.logJob("CURRENT", "OK", mallaEspana.size(), 0, 0, duracion, null);
    }

    // -------------------------------------------------------
    // JOB 3: HISTÓRICO (cada día a las 3:30am - archiva ayer)
    // -------------------------------------------------------
    @Scheduled(cron = "${meteo.jobs.historical-cron}")
    public void jobHistorico() {
        String aWeekAgo = LocalDate.now().minusDays(7)
                .format(DateTimeFormatter.ISO_LOCAL_DATE);
        log.info("=== JOB HISTÓRICO iniciado para fecha: {} ===", aWeekAgo);
        long inicio = System.currentTimeMillis();

        List<List<PuntoMalla>> batches = dividirEnBatches(mallaEspana, CHUNK_SIZE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (List<PuntoMalla> batch : batches) {
            CompletableFuture<Void> future = openMeteoClient
                    .getHistoricoBatch(batch, aWeekAgo, aWeekAgo)
                    .thenAccept(responses -> {
                        for (int i = 0; i < responses.length; i++) {
                            meteoRepository.upsertHistorico(batch.get(i), responses[i]);
                        }
                    })
                    .exceptionally(e -> {
                        log.error("Error en batch histórico: {}", e.getMessage());
                        return null;
                    });
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long duracion = System.currentTimeMillis() - inicio;
        log.info("=== JOB HISTÓRICO completado en {}ms ===", duracion);
        meteoRepository.logJob("HISTORICAL", "OK", mallaEspana.size(), 0, 0, duracion, null);
    }

    // -------------------------------------------------------
    // Utilidad: dividir lista en sublistas de tamaño n
    // -------------------------------------------------------
    private <T> List<List<T>> dividirEnBatches(List<T> lista, int tamano) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < lista.size(); i += tamano) {
            batches.add(lista.subList(i, Math.min(i + tamano, lista.size())));
        }
        return batches;
    }
}
