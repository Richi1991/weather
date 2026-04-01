package com.meteo.service;

import com.meteo.model.PuntoTemperatura;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.List;

@Service
public class RasterService {

    private static final double LAT_MIN = 27.0;
    private static final double LAT_MAX = 44.0;
    private static final double LON_MIN = -18.5;
    private static final double LON_MAX =  4.5;
    private static final double PASO    = 0.05;

    // Dimensiones del PNG
    private static final int COLS = (int) Math.round((LON_MAX - LON_MIN) / PASO) + 1;
    private static final int ROWS = (int) Math.round((LAT_MAX - LAT_MIN) / PASO) + 1;

    /**
     * Convierte el grid interpolado a PNG
     * Cada píxel = un punto del grid
     * Color del píxel = temperatura según escala Windy
     */
    public byte[] generarPng(List<PuntoTemperatura> grid) throws Exception {
        BufferedImage img = new BufferedImage(COLS, ROWS, BufferedImage.TYPE_INT_ARGB);

        for (PuntoTemperatura punto : grid) {
            int col = (int) Math.round((punto.longitud() - LON_MIN) / PASO);
            int row = (int) Math.round((LAT_MAX - punto.latitud()) / PASO); // invertir Y

            if (col >= 0 && col < COLS && row >= 0 && row < ROWS) {
                int color = temperaturaAColor(punto.temperatura());
                img.setRGB(col, row, color);
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(img, "PNG", baos);
        return baos.toByteArray();
    }

    private int temperaturaAColor(double temp) {
        int[][] escala = {
                {-20, 130, 22,  146},
                {-10, 11,  36,  251},
                {  0, 0,   190, 255},
                {  5, 0,   255, 170},
                { 10, 0,   220, 0  },
                { 15, 160, 220, 0  },
                { 20, 255, 230, 0  },
                { 25, 255, 150, 0  },
                { 30, 255, 50,  0  },
                { 40, 128, 0,   0  }
        };

        if (temp <= escala[0][0])
            return new Color(escala[0][1], escala[0][2], escala[0][3], 200).getRGB();
        if (temp >= escala[escala.length-1][0])
            return new Color(escala[escala.length-1][1],
                    escala[escala.length-1][2],
                    escala[escala.length-1][3], 200).getRGB();

        for (int i = 0; i < escala.length - 1; i++) {
            if (temp >= escala[i][0] && temp <= escala[i+1][0]) {
                double t = (temp - escala[i][0]) / (escala[i+1][0] - escala[i][0]);
                int r = (int) (escala[i][1] + t * (escala[i+1][1] - escala[i][1]));
                int g = (int) (escala[i][2] + t * (escala[i+1][2] - escala[i][2]));
                int b = (int) (escala[i][3] + t * (escala[i+1][3] - escala[i][3]));
                return new Color(r, g, b, 200).getRGB();
            }
        }
        return new Color(255, 255, 255, 200).getRGB();
    }
}