package com.apex.operations.console;

import java.time.Instant;
import org.springframework.http.*;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.annotation.*;

/** Error responses never echo URI, query input, payload, credentials or exception text. */
@RestControllerAdvice
public final class SafeErrors {
 public record Failure(Instant asOf,String code,String uncertainty) {}
 @ExceptionHandler(Exception.class)
 public ResponseEntity<Failure> failed(Exception error) {
  HttpStatusCode status=error instanceof ErrorResponse known?known.getStatusCode():HttpStatus.INTERNAL_SERVER_ERROR;
  String code=switch(status.value()){case 400->"INVALID_QUERY";case 403->"SCOPE_DENIED";case 404->"UNKNOWN_ROUTE";default->"UNKNOWN";};
  return ResponseEntity.status(status).body(new Failure(Instant.now(),code,"UNKNOWN"));
 }
}
