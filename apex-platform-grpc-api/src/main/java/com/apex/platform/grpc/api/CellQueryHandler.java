package com.apex.platform.grpc.api;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CellQueryHandler {
  String type();

  int version();
}
