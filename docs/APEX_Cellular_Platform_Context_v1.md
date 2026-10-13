# APEX Cellular Distributed Platform

## Project Context & Continuation Document

Version: 1.0

## Project Vision

APEX is an enterprise Cellular Distributed Platform, not a single
business application.

The goal is to connect thousands of independent services (Cells) through
reliable event-driven communication.

Main goals:

-   Handle thousands of concurrent transactions.
-   Provide reliable asynchronous communication.
-   Enable independent scaling.
-   Support distributed workflows.
-   Provide failure recovery and compensation.
-   Build scalable read models for reporting.

## Cellular Architecture

Each business capability is modeled as a Cell.

A Cell owns:

-   Its domain
-   Its data
-   Its events
-   Its queries
-   Its read models

Core platform:

-   Event Backbone
-   Outbox Engine
-   Event Router
-   Query Gateway
-   Projection Engine
-   Saga Engine
-   Workflow Engine
-   Observability

## Current Status

Completed:

-   Event Envelope foundation
-   Integration Events
-   Outbox Pattern
-   Runtime Outbox Publisher Scheduler
-   Purchase transactional flow
-   Saga infrastructure foundation

The startup-only Outbox publishing problem was fixed. Events now publish
while the application is running without restart.

## Purchase Domain

Purchase is currently an atomic transaction:

Purchase API -\> Application Service -\> Unit Of Work -\> Financial
Transaction -\> Ledger -\> Purchase Completed Event -\> Outbox

Purchase should not become a Saga unless it crosses independent Cells.

## Saga Status

Saga infrastructure exists:

-   Saga Instance
-   Repository
-   Context
-   Recovery
-   Timeout
-   Metrics
-   Claim mechanism

Next evolution:

Convert Saga into a generic Workflow Engine instead of business-specific
Saga classes.

## Future Platform Components

### Event Platform

-   Event Router
-   Consumer SDK
-   Subscription Management
-   Schema Versioning

### Query Platform

-   Query Gateway
-   Query Contracts
-   Projection Engine
-   Read Models

### Cellular Governance

Each Cell defines:

-   Published Events
-   Consumed Events
-   Queries
-   Data Ownership

## Roadmap

Phase 1: Cell Contract Model

Phase 2: Event Communication Platform

Phase 3: Query and Projection Platform

Phase 4: Generic Workflow/Saga Engine

Phase 5: Multi Cell Demonstration: - Credit Account - Credit Risk -
Shipping - Inventory - Payment - Reporting

## Architectural Rules

Every change must answer:

1.  Is this a Core Platform capability or a business feature?
2.  Is this a Cell responsibility or Platform responsibility?
3.  Is communication Event-based or Query-based?
4.  Who owns the data?
5.  Can a new Cell be added without modifying Core?

## Next Step

Do not build Purchase Saga yet.

The next architectural step is:

Design and implement the Cell Contract and Event Communication Model.
