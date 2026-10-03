package com.apex.apexbootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.apex")
public class ApexBootstrapApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApexBootstrapApplication.class, args);
    }

}
