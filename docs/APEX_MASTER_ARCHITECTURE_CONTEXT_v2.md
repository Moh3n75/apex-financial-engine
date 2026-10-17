# APEX Cellular Distributed Platform

# Master Architecture Context & Continuation Guide

Version: 2.0

------------------------------------------------------------------------

# 1. Executive Summary

APEX is an enterprise-scale Cellular Distributed Platform.

It is not designed as a single business application. Business domains
such as Credit, Shipping, Payment, Inventory and Reporting are examples
of Cells that run on top of a shared communication and workflow
foundation.

The primary objective is to build a platform capable of supporting:

-   Thousands of independent services.
-   Hundreds of millions of transaction records.
-   High-volume event processing.
-   Distributed workflows.
-   Independent scaling of business capabilities.
-   Reliable communication between services.
-   Large-scale reporting without affecting operational systems.

The platform follows these main architectural principles:

-   Cellular Architecture
-   Event Driven Architecture
-   Domain Ownership
-   Database Ownership Per Cell
-   CQRS / Read Model Separation
-   Generic Workflow and Saga Orchestration
-   Platform Capabilities Over Business Features

------------------------------------------------------------------------

# 2. Core Vision

The final system should look like:

                        APEX PLATFORM CORE


     -----------------------------------------------------

     Event Backbone

     Outbox Engine

     Event Router

     Consumer Framework

     Schema Registry

     Query Gateway

     Projection Engine

     Workflow Engine

     Saga Orchestrator

     Cell Registry

     Observability Platform

     -----------------------------------------------------


            |              |              |

            v              v              v


         Credit        Shipping       Inventory

          Cell           Cell            Cell


            |

         Payment

          Cell

The platform core should remain independent from business domains.

------------------------------------------------------------------------

# 3. Meaning of Cellular Architecture

A Cell is an independently owned business capability.

A Cell owns:

-   Domain logic
-   Database
-   Commands
-   Events
-   Queries
-   Read models
-   Internal workflows

A Cell does NOT directly access another Cell database.

Wrong:

    Shipping Service

            |

            v

    Credit Database

Correct:

    Shipping Cell

            |

     Query / Event Contract

            |

    Credit Cell

------------------------------------------------------------------------

# 4. Why Credit Was Added

Credit is not the final product.

Credit was created as a realistic domain laboratory to validate the
platform.

It provides:

-   Real financial transactions.
-   Account operations.
-   Ledger.
-   Transaction lifecycle.
-   Events.
-   Outbox usage.
-   Future Saga scenarios.

The purpose was architectural validation.

------------------------------------------------------------------------

# 5. Important Architectural Decision

Credit itself is not necessarily one Cell.

A large domain can become multiple Cells.

Example:

    Credit Domain


           +----------------+
           |                |
           v                v


    Credit Account      Credit Risk

    Cell                Cell


           |

    Credit Decision

    Cell

Cell boundaries are based on:

-   Ownership
-   Capability
-   Scaling needs
-   Independent evolution

not naming.

------------------------------------------------------------------------

# 6. Current Implemented Components

## 6.1 Transaction Core

The current Purchase flow is a local atomic transaction.

Flow:

    Purchase API

            |

    Purchase Application Service

            |

    Unit Of Work

            |

    Financial Transaction

            |

    Debit Account

            |

    Credit Account

            |

    Ledger Creation

            |

    Purchase Completed Event

            |

    Outbox

This transaction should remain atomic.

It should not become a Saga unless it crosses independent Cells.

------------------------------------------------------------------------

# 7. Event Architecture

The Event Platform is the communication backbone.

Responsibilities:

-   Asynchronous communication
-   Decoupling services
-   Reliable delivery
-   Version management
-   Traceability

An Event contains:

-   Event ID
-   Correlation ID
-   Event Type
-   Version
-   Timestamp
-   Payload
-   Metadata

Example:

    PaymentCompleted.v1

    ShipmentCreated.v1

    InventoryReserved.v1

------------------------------------------------------------------------

# 8. Outbox Engine

Problem solved:

Dual write problem.

Incorrect:

    Save Database

    +

    Publish Event

because one operation may succeed and the other fail.

Solution:

    Business Transaction

            |

    Database Commit

            |

    Outbox Record

            |

    Publisher Worker

            |

    Event Backbone

------------------------------------------------------------------------

# 9. Outbox Implementation Status

Completed:

-   Outbox Storage
-   Event Envelope
-   Runtime Publisher
-   Scheduler Based Publishing
-   Publish Verification

Important bug fixed:

Previous behavior:

    Application Start

            |

    Publish Events

Cause:

Startup demo publisher.

Solution:

-   Removed startup publisher.
-   Added runtime scheduler.

Validated:

    POST /api/v1/purchases

            |

    Purchase Created

            |

    Outbox Created

            |

    Event Published

            |

    Consumer Received

No application restart required.

------------------------------------------------------------------------

# 10. Consumer Platform Future Design

Consumers should not implement messaging infrastructure manually.

Target:

    @EventHandler("PaymentCompleted")
    public void handle(Event event)

The platform handles:

-   Subscription
-   Retry
-   Offset
-   Error handling
-   Metrics
-   Tracing

------------------------------------------------------------------------

# 11. Query Architecture

Operational databases must not handle heavy reporting.

For large data volumes:

100M+ transactions require separation.

Architecture:

    Write Database

            |

    Events

            |

    Projection Engine

            |

    Read Database

            |

    Reporting / Analytics

------------------------------------------------------------------------

# 12. Projection Engine

Purpose:

Create optimized read models.

Example:

Input:

    TransactionCreated

Output:

    CustomerBalanceView

    DailyTransactionReport

    RiskDashboard

Read models are optimized for queries, not transactions.

------------------------------------------------------------------------

# 13. Query Gateway

Cells communicate through query contracts.

Goal:

Avoid database coupling.

Example:

    Shipping Cell

            |

    Query Gateway

            |

    Customer Cell

------------------------------------------------------------------------

# 14. Saga and Workflow Architecture

Saga is not a business class.

Wrong:

    PurchaseSaga.java

Correct:

    Generic Workflow Engine


    Workflow Definition

            |

    Execution Engine

            |

    State Machine

            |

    Compensation Rules

------------------------------------------------------------------------

# 15. Current Saga Status

Implemented foundation:

-   Saga Instance
-   Saga Repository
-   Saga Context
-   Recovery Service
-   Timeout Handling
-   Metrics
-   Claim Mechanism

Not completed:

-   Generic workflow definition
-   Step registry
-   Compensation engine
-   Multi-cell orchestration

------------------------------------------------------------------------

# 16. Scaling Requirements

The platform must support:

-   10K+ transactions per second evolution path.
-   100M+ transaction records.
-   Large event streams.
-   Independent Cell scaling.

Required concepts:

-   Partitioning
-   Consumer groups
-   Backpressure
-   Data lifecycle management
-   Read/write separation

------------------------------------------------------------------------

# 17. Development Roadmap

## Phase 1 - Cell Contract Model

Build:

-   Cell metadata
-   Ownership model
-   Published events
-   Consumed events
-   Query contracts

------------------------------------------------------------------------

## Phase 2 - Event Platform

Build:

-   Event Router
-   Consumer SDK
-   Subscription Management
-   Schema Registry

------------------------------------------------------------------------

## Phase 3 - Query Platform

Build:

-   Query Gateway
-   Projection Framework
-   Read Model Management

------------------------------------------------------------------------

## Phase 4 - Generic Workflow Platform

Upgrade Saga:

-   Workflow Definitions
-   Step Execution
-   Compensation
-   Recovery

------------------------------------------------------------------------

## Phase 5 - Multi Cell Validation

Create:

-   Credit Account Cell
-   Credit Risk Cell
-   Payment Cell
-   Shipping Cell
-   Inventory Cell
-   Reporting Cell

------------------------------------------------------------------------

# 18. Rules For Future Development

Before adding any feature:

1.  Is this a platform capability or business capability?
2.  Should this belong to a Cell or Core?
3.  Who owns the data?
4.  Should communication use Event or Query?
5.  Can a new Cell be added without changing Core?

------------------------------------------------------------------------

# 19. Current Exact Position

Completed:

-   Transaction Core
-   Purchase Domain
-   Event Envelope
-   Outbox Pattern
-   Runtime Publisher
-   Basic Saga Infrastructure

Next:

1.  Cell Contract Model
2.  Event Communication Model
3.  Consumer Framework
4.  Query Gateway
5.  Projection Engine
6.  Generic Workflow Engine
7.  Multi Cell Testing

------------------------------------------------------------------------

# 20. Immediate Next Action

Do not implement Purchase Saga.

The next architectural task is:

Design the APEX Cell Contract and Event Communication Model.

This becomes the foundation for every future service.
