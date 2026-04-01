package com.meteo.job;

import com.meteo.client.OpenMeteoClient;
import com.meteo.common.Constantes;
import com.meteo.config.MallaConfig;
import com.meteo.dto.CurrentGridDTO;
import com.meteo.model.OpenMeteoResponse;
import com.meteo.model.PuntoMalla;
import com.meteo.model.PuntoTemperatura;
import com.meteo.repository.MeteoRepository;
import com.meteo.repository.RedisCurrentRepository;
import com.meteo.repository.RedisRasterRepository;
import com.meteo.service.InterpolacionService;
import com.meteo.service.RasterService;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.meteo.common.Constantes.BATCH_SIZE;

@Service
public class MeteoJobService {

    private static final Logger log = LoggerFactory.getLogger(MeteoJobService.class);

    @Autowired
    private OpenMeteoClient openMeteoClient;

    @Autowired
    private MeteoRepository meteoRepository;

    private final List<PuntoMalla> mallaEspana;

    @Autowired
    private DSLContext dsl;

    @Autowired
    private RedisCurrentRepository redisCurrentRepository;

    @Autowired
    private InterpolacionService interpolacionService;

    @Autowired
    private RasterService rasterService;

    @Autowired
    private RedisRasterRepository redisRasterRepository;

    public MeteoJobService(List<PuntoMalla> mallaEspana) {
        this.mallaEspana = mallaEspana;
    }

    @Scheduled(fixedRate = 600000)
    public void keepRenderDespierto() {
        try {
            // Una consulta que no pesa nada pero cuenta como actividad
            dsl.selectOne().execute();
            // Solo para que lo veas en los logs de Render al principio
            System.out.println(">>> Keep-Alive: Despertando a Render.");
        } catch (Exception e) {
            // Si falla, probablemente es que Neon ya se estaba durmiendo,
            // la siguiente ejecución lo despertará.
        }
    }

    // -------------------------------------------------------
    // JOB 1: FORECAST (cada 6 horas, sincronizado con Open-Meteo)
    // -------------------------------------------------------
//    @Scheduled(cron = "${meteo.jobs.forecast-cron}")
//    public void jobForecast() {
//        log.info("=== JOB FORECAST iniciado. {} puntos totales ===", mallaEspana.size());
//        long inicio = System.currentTimeMillis();
//        int puntosOk = 0;
//        int puntosError = 0;
//
//        List<List<PuntoMalla>> batches = dividirEnBatches(mallaEspana, BATCH_SIZE);
//        List<CompletableFuture<Void>> futures = new ArrayList<>();
//
//        for (List<PuntoMalla> batch : batches) {
//            CompletableFuture<Void> future = openMeteoClient
//                    .getForecastBatch(batch)
//                    .thenAccept(responses -> {
//                        for (int i = 0; i < responses.length; i++) {
//                            meteoRepository.upsertForecast(batch.get(i), responses[i]);
//                            try {
//                                Thread.sleep(200);
//                            } catch (InterruptedException e) {
//                                log.warn("Error guardando punto {}: {}", batch.get(i), e.getMessage());
//                            }
//                            log.info("Batch {} OK — {} puntos guardados en Redis", batch.size());
//                        }
//                    })
//                    .exceptionally(e -> {
//                        log.error("Error en batch forecast: {}", e.getMessage());
//                        return null;
//                    });
//            futures.add(future);
//        }
//
//        // Esperar a todos los batches
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//
//        long duracion = System.currentTimeMillis() - inicio;
//        log.info("=== JOB FORECAST completado en {}ms ===", duracion);
//        meteoRepository.logJob("FORECAST", "OK", mallaEspana.size(), puntosOk, puntosError, duracion, null);
//    }

    // -------------------------------------------------------
    // JOB 2: DATOS ACTUALES (cada hora)
    // -------------------------------------------------------
    @Scheduled(cron = "${meteo.jobs.current-cron}")
    public void jobCurrent() {
        log.info("=== JOB CURRENT iniciado ===");
        long inicio = System.currentTimeMillis();

        List<List<PuntoMalla>> batches = dividirEnBatches(mallaEspana, BATCH_SIZE);

        AtomicInteger puntosOk    = new AtomicInteger(0);
        AtomicInteger puntosError = new AtomicInteger(0);
        AtomicInteger batchesOk   = new AtomicInteger(0);
        AtomicInteger batchesError = new AtomicInteger(0);

        for (int b = 0; b < batches.size(); b++) {
            List<PuntoMalla> batch = batches.get(b);
            int intentos = 0;
            int maxIntentos = 3;
            boolean exito = false;

            while (intentos < maxIntentos && !exito) {
                try {
                    
                    OpenMeteoResponse[] responses = openMeteoClient
                            .getCurrentBatch(batch)
                            .get();

                    for (int i = 0; i < responses.length; i++) {
                        try {
                            redisCurrentRepository.saveCurrent(batch.get(i), responses[i]);
                            puntosOk.incrementAndGet();
                        } catch (Exception e) {
                            puntosError.incrementAndGet();
                            log.warn("Error guardando punto {}: {}", batch.get(i), e.getMessage());
                        }
                    }

                    batchesOk.incrementAndGet();
                    exito = true;
                    Thread.sleep(15_000);

                } catch (Exception e) {
                    intentos++;
                    if (e.getMessage() != null && e.getMessage().contains("429")) {
                        long espera = 2000L * intentos; // 2s, 4s, 6s
                        log.warn("429 en batch {} — reintentando en {}ms (intento {}/{})",
                                b, espera, intentos, maxIntentos);
                        try { Thread.sleep(espera); } catch (InterruptedException ignored) {}
                    } else {
                        log.error("Error en batch current {}: {}", b, e.getMessage());
                        break;
                    }
                }
            }
            if (!exito && intentos >= maxIntentos) {
                log.error("Batch {} fallido tras {} intentos — {} puntos perdidos",
                        b, maxIntentos, batch.size());
                batchesError.incrementAndGet();
                puntosError.addAndGet(batch.size());
            }
        }

        long duracion = System.currentTimeMillis() - inicio;

        log.info("=== JOB CURRENT completado en {}ms ===", duracion);
        log.info("    Batches → OK: {} | ERROR: {} | Total: {}",
                batchesOk.get(), batchesError.get(), batches.size());
        log.info("    Puntos  → OK: {} | ERROR: {} | Total: {}",
                puntosOk.get(), puntosError.get(), mallaEspana.size());

        meteoRepository.logJob(
                "CURRENT",
                batchesError.get() == 0 ? "OK" : "PARTIAL",
                mallaEspana.size(),
                puntosOk.get(),
                puntosError.get(),
                duracion,
                batchesError.get() > 0 ? batchesError.get() + " batches fallidos" : null
        );
        generarRasterTemperatura();
    }

    private void generarRasterTemperatura() {
        try {
            log.info("Generando raster de temperatura...");

            // 2. Convertir a PuntoTemperatura
            List<PuntoTemperatura> puntos = redisCurrentRepository.getAllCurrentEspana()
                    .stream()
                    .map(p -> new PuntoTemperatura(
                            ((Number) p.get("latitud")).doubleValue(),
                            ((Number) p.get("longitud")).doubleValue(),
                            ((Number) p.getOrDefault("temperature_2m", 15.0)).doubleValue()
                    ))
                    .toList();

            // 3. Interpolar a 0.05°
            List<PuntoTemperatura> grid = interpolacionService.interpolarGrid(puntos);
            log.info("Grid interpolado: {} puntos", grid.size());

            // 4. Generar PNG
            byte[] png = rasterService.generarPng(grid);
            log.info("PNG generado: {} bytes", png.length);

            // 5. Guardar en Redis
            redisRasterRepository.saveRasterTemperatura(png);

        } catch (Exception e) {
            log.error("Error generando raster: {}", e.getMessage());
        }
    }

    // -------------------------------------------------------
    // JOB 3: HISTÓRICO (cada día a las 3:30am - archiva ayer)
    // -------------------------------------------------------
//    @Scheduled(cron = "${meteo.jobs.historical-cron}")
//    public void jobHistorico() {
//        String aWeekAgo = LocalDate.now().minusDays(7)
//                .format(DateTimeFormatter.ISO_LOCAL_DATE);
//        log.info("=== JOB HISTÓRICO iniciado para fecha: {} ===", aWeekAgo);
//        long inicio = System.currentTimeMillis();
//
//        List<List<PuntoMalla>> batches = dividirEnBatches(mallaEspana, Constantes.CHUNK_SIZE);
//        List<CompletableFuture<Void>> futures = new ArrayList<>();
//
//        for (List<PuntoMalla> batch : batches) {
//            CompletableFuture<Void> future = openMeteoClient
//                    .getHistoricoBatch(batch, aWeekAgo, aWeekAgo)
//                    .thenAccept(responses -> {
//                        for (int i = 0; i < responses.length; i++) {
//                            meteoRepository.upsertHistorico(batch.get(i), responses[i]);
//                            try {
//                                Thread.sleep(200);
//                            } catch (InterruptedException e) {
//                                throw new RuntimeException(e);
//                            }
//                        }
//                    })
//                    .exceptionally(e -> {
//                        log.error("Error en batch histórico: {}", e.getMessage());
//                        return null;
//                    });
//            futures.add(future);
//        }
//
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
//
//        long duracion = System.currentTimeMillis() - inicio;
//        log.info("=== JOB HISTÓRICO completado en {}ms ===", duracion);
//        meteoRepository.logJob("HISTORICAL", "OK", mallaEspana.size(), 0, 0, duracion, null);
//    }

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
