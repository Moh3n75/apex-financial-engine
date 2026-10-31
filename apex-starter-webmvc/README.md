# APEX WebMVC Starter

Add `com.apex:apex-starter-webmvc:${apex.platform.version}` to an independently built Spring Boot 4.1.1 / Java 21 HTTP Cell.
It supplies Boot WebMVC, the shared HTTP execution-context filter, and the canonical
`apex-spring-boot-starter` transitively. M35.6 folds the old `apex-platform-webmvc`
implementation into this module; Java packages and runtime behavior are unchanged.
It introduces no Kafka, Messaging, Outbox or Inbox dependency.

Set `spring.application.name` and `apex.cell.id` using the application's normal configuration.
The existing `ApexWebMvcAutoConfiguration` discovers and registers one HTTP execution context filter.
Each request receives a root context with a new correlation ID and no causation ID; the scope closes on completion or failure.
`apex.webmvc.context.enabled` retains its existing default of `true`.

This is a library dependency aggregator, with no production Java classes, replacement filter, auto-configuration imports or executable Boot archive.
Validation and Actuator remain explicit application choices.
