# APEX Cell Template Generator Specification

Version: 1.0

## Goal

Create a production-ready repository template:

`apex-cell-template`

This template is the foundation for all future APEX Cells.

Developers using this template should only implement business capability
logic. Platform capabilities such as events, security, observability and
deployment integration must have predefined extension points.

------------------------------------------------------------------------

# Architecture Rules

The generated template MUST follow:

1.  Independent Cell Application model.
2.  Module-Based Architecture.
3.  Domain isolation.
4.  Database ownership per Cell.
5.  Contract-based communication.
6.  No business logic inside shared infrastructure.
7.  Platform concerns separated from domain code.

------------------------------------------------------------------------

# Technology

Use:

-   Java
-   Spring Boot
-   Maven

Prepare integration points for:

-   APEX Cell SDK
-   Event Platform
-   Query Gateway
-   Security Platform
-   Observability Platform

------------------------------------------------------------------------

# Repository Structure

Generate:

    apex-cell-template

    ├── pom.xml
    ├── README.md
    ├── Dockerfile
    ├── .gitignore
    │
    ├── src
    │   ├── main
    │   │   ├── java
    │   │   └── resources
    │   │       ├── application.yml
    │   │       ├── application-dev.yml
    │   │       └── application-prod.yml
    │   │
    │   └── test
    │
    ├── modules
    ├── security
    ├── observability
    ├── configuration
    └── deployment

------------------------------------------------------------------------

# Module Template

Every business module must have:

    modules/<module-name>

    ├── api
    ├── application
    ├── domain
    ├── infrastructure
    ├── events
    └── queries

------------------------------------------------------------------------

# Layer Responsibilities

## api

Contains:

-   Controllers
-   Request DTOs
-   Response DTOs

No business logic.

------------------------------------------------------------------------

## application

Contains:

-   Use cases
-   Application services
-   Command handlers

Responsible for orchestration.

------------------------------------------------------------------------

## domain

Contains:

-   Entities
-   Value Objects
-   Domain Services
-   Business Rules

Must not depend on:

-   Database
-   HTTP
-   Messaging
-   Framework infrastructure

------------------------------------------------------------------------

## infrastructure

Contains:

-   Repository implementations
-   Database adapters
-   External clients
-   Technical integrations

------------------------------------------------------------------------

## events

Contains:

-   Published events
-   Consumed events
-   Event contracts

Events must be immutable and versioned.

------------------------------------------------------------------------

## queries

Contains:

-   Query objects
-   Query handlers
-   Read models

------------------------------------------------------------------------

# Platform Integration Points

Prepare interfaces/placeholders for:

## Event Publishing

Use:

``` java
eventPublisher.publish(event);
```

Never directly use:

-   KafkaTemplate
-   RabbitTemplate

inside domain/application code.

------------------------------------------------------------------------

## Event Consumption

Prepare:

``` java
@EventHandler("EventName")
public void handle(Event event)
```

------------------------------------------------------------------------

# Security Package

Create:

    security

Prepare:

-   SecurityContext
-   AuthorizationPolicy
-   Permission model

------------------------------------------------------------------------

# Observability Package

Create:

    observability

Prepare:

-   Logging
-   Metrics
-   Distributed tracing

Every operation should support:

-   traceId
-   correlationId
-   cellId
-   eventId

------------------------------------------------------------------------

# Configuration

Create:

    configuration

Support:

-   Environment configuration
-   Feature flags
-   External service configuration

Never store secrets in source code.

------------------------------------------------------------------------

# Deployment

Create:

    deployment

    ├── docker
    ├── kubernetes
    └── manifests

Prepare:

-   Deployment
-   Service
-   ConfigMap
-   Secret

------------------------------------------------------------------------

# Database Preparation

Prepare:

    infrastructure/database

with:

-   Repository pattern
-   Migration support
-   Database configuration

Each Cell owns its database.

------------------------------------------------------------------------

# Testing

Create:

    src/test

    ├── unit
    ├── integration
    └── architecture

Support:

-   Domain tests
-   API tests
-   Integration tests

------------------------------------------------------------------------

# README Template

Generate:

    # Cell Name

    ## Responsibility

    ## Owned Data

    ## Published Events

    ## Consumed Events

    ## APIs

    ## Queries

    ## Deployment

    ## Local Development

------------------------------------------------------------------------

# Validation

After generation verify:

1.  Project builds.
2.  Spring Boot starts.
3.  No business logic exists.
4.  Template can create customer-cell.
5.  Template can create payment-cell.
6.  Template supports future APEX SDK integration.

------------------------------------------------------------------------

# Important

Do not create:

-   Customer logic
-   Payment logic
-   Shipping logic

This repository is only a reusable Cell foundation.
