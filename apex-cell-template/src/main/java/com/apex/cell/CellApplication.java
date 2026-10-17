package com.apex.cell;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Minimal Cell bootstrap. Future business code lives under
 * {@code com.apex.cell.modules.<module>.<layer>} — not as separate Maven modules.
 */
@SpringBootApplication
public class CellApplication {

    public static void main(String[] args) {
        SpringApplication.run(CellApplication.class, args);
    }
}
