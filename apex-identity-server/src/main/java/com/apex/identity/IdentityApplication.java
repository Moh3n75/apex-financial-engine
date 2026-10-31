package com.apex.identity;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IdentityApplication {
  public static void main(String[] args) {
    var app = new SpringApplication(IdentityApplication.class);
    app.addInitializers(context -> IdentityPolicy.validate(context.getEnvironment()));
    app.run(args);
  }
}
