package com.apex.platform.grpc.api;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface CellQuery {
  String type();

  int version();

  Class<?> response();
}
