package com.apex.apexbootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.apex")
@EnableScheduling
public class ApexBootstrapApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApexBootstrapApplication.class, args);
    }

}
