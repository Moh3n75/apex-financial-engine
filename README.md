# APEX FINANCIAL ENGINE — PROJECT HANDOFF
تاریخ وضعیت: 2026-10-03

این فایل برای ادامه پروژه در یک چت/جلسه جدید تهیه شده است.
هدف: با آپلود همین فایل، بتوان توسعه پروژه را دقیقاً از همین نقطه ادامه داد، بدون اینکه لازم باشد دوباره تصمیمات معماری، ساختار پروژه، وضعیت فعلی و مسیر بعدی توضیح داده شود.

---

# 1) هدف کلان پروژه

نام پروژه:
`apex-financial-engine`

هدف:
ساخت یک Financial / Credit Transaction Engine سازمانی، مقیاس‌پذیر، ماژولار و Cell-ready برای استفاده در یک اکوسیستم بزرگ با حداقل 36 میکروسرویس.

ویژگی‌های کلان هدف:
- تراکنش‌های مالی و اعتباری
- Credit Account
- چند نوع Credit
- Purchase / Transfer / Refund
- Hold / Block / Reservation
- Ledger
- Double-entry / Balanced Ledger
- Transactional Outbox
- Kafka
- Redis
- PostgreSQL
- jOOQ
- Idempotency
- Optimistic Concurrency
- Cell Architecture
- Reporting / OLAP / Big Data
- IoT / External Systems در آینده
- MQS / Unit-based credits و Exchange Context در مراحل بعد
- قابلیت توسعه برای چندین نوع Business Rule و Policy

هدف معماری در مقیاس:
سیستم در آینده می‌تواند به چند Cell و چندین Microservice تقسیم شود.
هر Cell باید بتواند Transaction / Credit / Ledger محلی خودش را داشته باشد و از Cross-Cell DB Call جلوگیری شود.

---

# 2) تصمیم اصلی معماری Cell

مدل انتخاب‌شده:
## Model A

Ledger داخل هر Cell قرار دارد.

ساختار مفهومی:

Cell
- Transaction
- Credit
- CreditHold
- Ledger
- PostgreSQL
- Outbox
- Kafka publishing later

دلیل:
- حذف bottleneck ناشی از Ledger مرکزی
- حذف distributed DB transaction بین Cellها
- نگه داشتن consistency مالی داخل یک Cell
- گزارش‌گیری Global از طریق event streaming

Cross-cell operations در آینده:
- Saga
- Reservation / Hold
- Compensation
- Event-driven coordination

نه:
- XA / distributed DB transaction

---

# 3) مدل Persistence

مدل انتخاب‌شده:
## Hybrid Persistence

نه Pure Event Sourcing
نه CRUD ساده

هر Aggregate / Flow می‌تواند داشته باشد:
1. Current State
2. Domain Events
3. Immutable Ledger
4. Transactional Outbox
5. Async Read Models

Hot-path:
از Current State خوانده می‌شود.

Event Replay:
برای recovery / audit / rebuild / verification
نه برای هر درخواست عادی.

---

# 4) اصول مهم Domain

Domain باید Pure Java باشد.

در Domain:
- Spring annotation ممنوع
- JPA annotation ممنوع
- jOOQ ممنوع
- DB dependency ممنوع

Spring فقط در Outer Layers.

Aggregateها:
- رفتار را کنترل می‌کنند
- invariant را enforce می‌کنند
- state را از طریق method/event تغییر می‌دهند

Value Objectها:
- immutable

Domain Events:
- Factهایی هستند که در Domain اتفاق افتاده‌اند
- با Integration Event فرق دارند

---

# 5) ساختار Maven فعلی

ساختار درست پروژه:

apex-financial-engine/
├── apex-domain
├── apex-application
├── apex-infrastructure
└── apex-bootstrap

Dependency direction:

apex-domain
↑
apex-application
↑
apex-infrastructure
↑
apex-bootstrap

یعنی:

application -> domain
infrastructure -> application
bootstrap -> infrastructure

و:

domain -> هیچ ماژول داخلی دیگری

نکته:
قبلاً application package اشتباهاً داخل apex-domain ساخته شده بود.
این مشکل اصلاح شد.

الان:
`com.apex.credit.application.*`
فقط داخل apex-application است.

---

# 6) نسخه‌ها و تکنولوژی‌های فعلی

Java:
- Java 21

Spring Boot:
- 4.1.1

jOOQ:
- 3.21.7
- با Spring Boot 4.1.1 هماهنگ شده

Database:
- PostgreSQL

Migration:
- Flyway

Runtime:
- apex-bootstrap

Web:
- Spring MVC
- برای Boot 4 انتخاب ترجیحی:
  `spring-boot-starter-webmvc`

Validation:
- `spring-boot-starter-validation`

Persistence Adapter:
- jOOQ

---

# 7) نکته مهم jOOQ Code Generation

Generated Code نباید Commit شود.

Generated code location:

apex-infrastructure/
└── target/
└── generated-sources/
└── jooq/
└── com/apex/infrastructure/jooq/generated/

ساختار فعلی Generated:

generated/
├── Tables.java
├── DefaultCatalog.java
├── Public.java
├── Indexes.java
├── Keys.java
├── tables/
│   ├── CreditAccountState.java
│   ├── CreditHold.java
│   ├── CreditReservation.java
│   ├── FinancialTransaction.java
│   ├── LedgerEntry.java
│   ├── LedgerTransaction.java
│   └── OutboxEvent.java
└── tables/records/
├── CreditAccountStateRecord.java
├── CreditHoldRecord.java
├── CreditReservationRecord.java
├── FinancialTransactionRecord.java
├── LedgerEntryRecord.java
├── LedgerTransactionRecord.java
└── OutboxEventRecord.java

Generated Sources در IntelliJ به عنوان:
Generated Sources Root
شناخته شده است.

jOOQ plugin فقط در:
`apex-infrastructure/pom.xml`
قرار دارد.

jOOQ نباید در apex-domain باشد.

---

# 8) Flyway / DB schema فعلی

Migrationهای اصلی تا اینجا:

## V1
جداول اولیه:

### credit_account_state
مفهومی:
- id BIGSERIAL
- public_id UUID
- owner_id BIGINT
- credit_type VARCHAR
- available_units BIGINT
- blocked_units BIGINT
- version BIGINT
- timestamps

### financial_transaction
- id BIGSERIAL
- public_id UUID
- type
- status
- amount_units
- version
- timestamps

### ledger_transaction

### ledger_entry

### outbox_event

---

## V2
`credit_reservation`

این جدول در DB فعلی وجود دارد، ولی بعداً تصمیم معماری عوض شد:
Reservation فقط یک نوع Hold است.

بنابراین:
`credit_reservation` فعلاً legacy/prototype است و در آینده باید با migration جدید deprecate/drop/migrate شود.
Migration applied را نباید ویرایش کرد.

---

## V3
`credit_hold`

CreditHold به عنوان Aggregate مستقل انتخاب شد.

مفهوم:
- public id
- credit account
- transaction
- hold type
- amount
- remaining
- consumed
- status
- references
- expiry
- version
- timestamps

---

## V4
برای هماهنگی Purchase persistence اضافه شد:

در `credit_account_state`:
- member_public_id UUID
- credit_type_public_id UUID

در `financial_transaction`:
- reference_id VARCHAR
- failure_reason VARCHAR

و Unique Index برای:
`financial_transaction.reference_id`

این Unique Constraint برای Idempotency استفاده می‌شود.

نام constraint/index فعلی:
`uq_financial_transaction_reference`

---

# 9) مدل Amount

Money(BigDecimal) برای Hot-path کنار گذاشته شد.

مدل:
`CreditAmount`

مبنای ذخیره:
fixed-point smallest units در `long`

مثلاً:
اگر scale = 4
12.3456 => 123456 units

اصول:
- no double
- no float
- Math.addExact / subtractExact
- scale مربوط به CreditType است
- precision rule باید explicit باشد

---

# 10) ID Strategy

مدل Hybrid ID:

Domain/Public IDs:
- UUID

DB internal IDs:
- BIGINT / BIGSERIAL

مثلاً:
CreditAccountId در Domain UUID-backed است.

در DB:
- id = BIGINT
- public_id = UUID

هدف:
- UUID برای distributed/public identity
- BIGINT برای DB joins/performance

---

# 11) CreditAccount Aggregate

موجود است.

پکیج:
`com.apex.credit.domain...`

رفتارهای فعلی شامل:
- create
- restore
- addCredit
- block
- debit

State فعلی:
- id
- memberId
- creditTypeId
- available
- blocked
- version
- uncommittedEvents

Event Pattern:

raise(event)
↓
apply(event)
↓
state changes
↓
uncommittedEvents

همچنین:
- persistedVersion()
- markChangesAsCommitted()

Domain events فعلی CreditAccount:
- CreditAccountCreatedEvent
- CreditAddedEvent
- CreditBlockedEvent
- CreditDebitedEvent

CreditDebitedEvent باید `aggregateId()` مطابق CreditAccountEvent داشته باشد.

---

# 12) CreditHold Aggregate

تصمیم معماری:
CreditHold یک Aggregate مستقل است.

CreditAccount فقط total state را نگه می‌دارد:
- available
- blocked

ولی Hold علت/چرخه lifecycle بلوکه شدن را نگه می‌دارد.

یک CreditAccount می‌تواند چند Hold همزمان داشته باشد.

مثال‌ها:
- SHIPPING_RESERVE
- USER_GIFT_BLOCK
- FACILITY_COLLATERAL
- TAX_HOLD
- PAYMENT_RESERVE
- GENERAL_BLOCK

CreditHold فعلی حداقل:
- create
- consume

Domain events فعلی:
- CreditHoldCreatedEvent
- CreditHoldConsumedEvent

Release / Expire طراحی شدند ولی به صورت کامل در flow end-to-end پیاده‌سازی نشده‌اند.

این Flow عمداً متوقف شد تا ابتدا یک Vertical Slice کامل Purchase ساخته شود.

---

# 13) FinancialTransaction Aggregate

موجود و فعال است.

مدل:
- TransactionId
- TransactionType
- TransactionStatus
- requestedAmount
- referenceId
- failureReason
- version
- uncommittedEvents

State Machine:

CREATED
↓
PROCESSING
↓
COMPLETED

یا:

FAILED

Domain events:
- TransactionCreatedEvent
- TransactionStartedEvent
- TransactionCompletedEvent
- TransactionFailedEvent

Transaction Aggregate خودش balance را تغییر نمی‌دهد.

---

# 14) Ledger Domain

پیاده‌سازی شده.

کلاس‌ها:
- LedgerEntryType
- LedgerEntry
- LedgerTransaction

LedgerEntryType:
- DEBIT
- CREDIT

Invariant مهم LedgerTransaction:

Total Debit == Total Credit

Ledger immutable است.

Repository operation:
`append()`

نه:
- update
- delete

Purchase فعلی:
Buyer -> DEBIT
Seller -> CREDIT

مثلاً:

Buyer   DEBIT   30000
Seller  CREDIT  30000

Balanced = true

---

# 15) Application Layer فعلی

در:
`apex-application`

Ports:
- CreditAccountRepository
- FinancialTransactionRepository
- LedgerRepository
- OutboxRepository
- UnitOfWork

Purchase:
- PurchaseCommand
- PurchaseResult
- PurchaseApplicationService

UnitOfWork:

```java
public interface UnitOfWork {
    <T> T execute(Supplier<T> operation);
}
```

Infrastructure implementation:
`SpringUnitOfWork`

با:
`TransactionTemplate`

بنابراین:
Application Layer Spring annotation ندارد.

---

# 16) Persistence Adapters فعلی

در:
`apex-infrastructure`

مهم‌ترین Adapterها:

- JooqCreditAccountRepository
- JooqFinancialTransactionRepository
- JooqLedgerRepository
- JooqOutboxRepository
- SpringUnitOfWork
- ApplicationConfiguration

نکته Spring:
Repositoryهای managed توسط Spring `final` نیستند.

قبلاً:
`JooqCreditAccountRepository` final بود
و Spring CGLIB نمی‌توانست subclass بسازد.

اصلاح شد.

---

# 17) Purchase Flow — وضعیت فعلی

این اولین Vertical Slice کامل پروژه است.

Flow:

HTTP
↓
PurchaseController
↓
PurchaseApplicationService
↓
UnitOfWork
↓
FinancialTransaction
↓
CreditAccount source
↓
CreditAccount destination
↓
Ledger
↓
Outbox
↓
PostgreSQL COMMIT

سناریوی تست:

Buyer:
100000

Seller:
10000

Purchase:
30000

نتیجه:
Buyer = 70000
Seller = 40000

financial_transaction:
PURCHASE / COMPLETED

Ledger:
DEBIT 30000
CREDIT 30000

Outbox:
PURCHASE_COMPLETED
published = false

---

# 18) Purchase HTTP API

پیاده شده.

Bootstrap Web Adapter:

مسیر مفهومی:
`apex-bootstrap/src/main/java/com/apex/apexbootstrap/api/purchase/`

کلاس‌ها:
- PurchaseController
- PurchaseRequest
- PurchaseResponse

Endpoint:

POST
`/api/v1/purchases`

Input شامل:
- sourceAccountId
- destinationAccountId
- amountUnits
- referenceId

Output:
- transactionId
- status

Spring MVC در Bootstrap استفاده شده.

---

# 19) Idempotency فعلی

Idempotency دو لایه دارد.

## Fast Path
در PurchaseApplicationService:

قبل از شروع عملیات:
`findByReferenceId(referenceId)`

اگر موجود بود:
همان Transaction result برگردانده می‌شود.

---

## Concurrent Gate
در DB:

`financial_transaction.reference_id`
Unique است.

در executeInsideTransaction:

1. FinancialTransaction ساخته می‌شود
2. start()
3. قبل از تغییر balance، transaction در DB با status PROCESSING ذخیره می‌شود
4. Unique constraint روی reference_id نقش Idempotency Gate را دارد
5. سپس accountها load و balance تغییر می‌کنند
6. ledger ثبت می‌شود
7. transaction.complete()
8. transaction update می‌شود
9. outbox ثبت می‌شود
10. COMMIT

اگر دو request همزمان referenceId یکسان داشته باشند:
فقط یکی INSERT موفق دارد.

---

# 20) Duplicate Exception Handling

Spring/jOOQ در Boot 4 duplicate DB constraint را به:

`org.springframework.dao.DuplicateKeyException`

ترجمه می‌کند.

JooqFinancialTransactionRepository این Exception را به:

`DuplicateRequestException`

تبدیل می‌کند.

مهم:
Catch اصلی DuplicateRequestException بیرون UnitOfWork انجام می‌شود.

دلیل:
در PostgreSQL بعد از Constraint Violation تراکنش فعلی باید rollback شود.

Flow:

DuplicateKeyException
↓
DuplicateRequestException
↓
UnitOfWork rollback
↓
execute() catches
↓
winner transaction loaded
↓
same result returned

---

# 21) تست‌های فعلی

## PurchaseIntegrationTest
سبز است.

بررسی:
- balance buyer
- balance seller
- financial_transaction
- ledger_transaction
- ledger_entry
- outbox_event

---

## ConcurrentPurchaseIntegrationTest
سبز است.

10 thread همزمان با referenceId یکسان.

انتظار و نتیجه:
- فقط یک Transaction
- فقط یک Balance mutation
- فقط یک LedgerTransaction
- فقط یک Outbox
- Buyer فقط یک بار debit
- Seller فقط یک بار credit

این تست بدون `@Transactional` نوشته شد چون هر thread باید transaction مستقل واقعی داشته باشد.

نتیجه:
Concurrent Idempotency اثبات شده است.

---

# 22) Optimistic Concurrency

JooqCreditAccountRepository از version استفاده می‌کند.

Update pattern:

WHERE public_id = ?
AND version = expectedVersion

اگر updated rows != 1:
concurrent modification detected

هدف:
جلوگیری از lost update

نکته معماری:
برای Hot Accountهای بسیار پر contention، optimistic locking + retry به تنهایی کافی نیست.
در آینده:
- serialized per-account processing
- partition/key ordering
- actor-like queue
- Kafka partitioning by account id
  ممکن است اضافه شود.

---

# 23) Transaction Boundary

Purchase فعلی در یک PostgreSQL Transaction انجام می‌شود.

داخل همان transaction:

- financial_transaction PROCESSING insert
- source account update
- destination account update
- ledger transaction
- ledger entries
- transaction COMPLETED update
- outbox insert

اگر هر کدام fail شود:
کل عملیات rollback می‌شود.

---

# 24) Outbox فعلی

Transactional Outbox پیاده شده.

فعلاً:
`JooqOutboxRepository`

OutboxMessage شامل:
- eventType
- aggregateId
- payload

Purchase event:

`PURCHASE_COMPLETED`

Payload فعلاً به صورت String JSON ساخته می‌شود.

مثلاً:
- transactionId
- referenceId
- status
- amountUnits

published:
false

Kafka publisher هنوز پیاده نشده.

---

# 25) Domain Event vs Integration Event

این تفکیک مهم است.

## Domain Event
داخل Aggregate.

مثلاً:
- CreditDebitedEvent
- CreditAddedEvent
- TransactionCompletedEvent

کار:
ثبت fact داخل Domain و state transition.

---

## Integration Event
برای ارتباط سرویس‌ها.

مثلاً:
- PURCHASE_COMPLETED

فعلاً از طریق Outbox نوشته می‌شود.

---

## Ledger
Event نیست.

Ledger:
immutable financial record است.

---

# 26) Event Infrastructure — وضعیت فعلی

هنوز Infrastructure مشترک Event ساخته نشده.

در آخرین تصمیم معماری پیشنهاد شد که به دلیل هدف 36 Microservice، یک Platform/Foundation مشترک ساخته شود.

پیشنهاد معماری:

apex-platform
├── apex-platform-core
├── apex-platform-events
├── apex-platform-outbox
├── apex-platform-idempotency
├── apex-platform-web
├── apex-platform-observability
└── apex-platform-kafka

اما:

## مهم
هیچ‌کدام از این ماژول‌های apex-platform هنوز پیاده‌سازی نشده‌اند.

این فقط تصمیم/پیشنهاد مرحله بعد است.

---

# 27) Platform/Foundation پیشنهادی برای 36 Microservice

دلیل:
نباید 36 سرویس هر کدام:
- Event handling
- Error handling
- Correlation ID
- Outbox
- Kafka
- Serialization
- Idempotency
- Trace
- Metrics
  را جداگانه و متفاوت پیاده کنند.

بهتر است Platform standards مشترک داشته باشیم.

ولی Business Logic نباید وارد Platform شود.

نباید این‌ها shared شوند:
- Purchase
- CreditHold business rules
- Facility
- Settlement
- Tax policy

فقط Technical Cross-Cutting Concerns مشترک می‌شوند.

---

# 28) Event Platform پیشنهادی — هنوز پیاده نشده

پیشنهاد:

## IntegrationEvent

Contract مشترک برای eventهایی که بین سرویس‌ها منتشر می‌شوند.

## EventEnvelope<T>

پیشنهاد metadata:

- eventId
- eventType
- eventVersion
- correlationId
- causationId
- aggregateId
- aggregateType
- sourceService
- cellId
- occurredAt
- payload

مثال مفهومی:

```java
public record EventEnvelope<T>(
    UUID eventId,
    String eventType,
    String eventVersion,
    UUID correlationId,
    UUID causationId,
    String sourceService,
    Instant occurredAt,
    T payload
) {}
```

اما این کد هنوز وارد پروژه نشده.

---

# 29) Error Handling

کاربر تصمیم گرفت فعلاً Error Handling استاندارد را عقب بیندازد.

پیشنهاد وجود داشت برای:
- ApiError
- GlobalExceptionHandler
- AccountNotFoundException
- Standard HTTP status mapping

اما این مرحله فعلاً اولویت ندارد.

در آینده ترجیح:
به جای util پراکنده، Platform module مثل:
`apex-platform-web`
برای Error Contract مشترک ساخته شود.

---

# 30) Util vs Platform Decision

کاربر خواست برای Errorها و موضوعات مشابه Utility مشترک داشته باشد.

تصمیم معماری بهتر:

نام `util` استفاده نشود برای مجموعه بزرگ cross-cutting concerns.

بهتر:
`apex-platform-*`

چون util معمولاً تبدیل به God-package می‌شود.

پیشنهاد:
- platform-core
- platform-events
- platform-outbox
- platform-idempotency
- platform-web
- platform-observability
- platform-kafka

---

# 31) Reporting / Big Data Architecture

OLTP PostgreSQL فقط transactional authoritative data.

گزارش‌های سنگین نباید روی DB Cell اجرا شوند.

Flow آینده:

Cell PostgreSQL
↓
Transactional Outbox
↓
Kafka
↓
Read Models / OLAP
↓
ClickHouse / Data Lake / Warehouse / Flink / Spark

Reporting باید مستقل Scale شود.

---

# 32) Business Complexity Reference

محصول جدید قرار نیست سیستم قدیمی را Copy کند.

ولی باید complexity مشابه یا بالاتر را پشتیبانی کند.

Business concepts مهم:
- GENERAL credit
- DONATION
- USER_GIFT
- Facility
- blocked variants
- Tax
- Manager accounts
- MQS
- PSP
- Settlement
- Voucher
- Expiry
- Multi-source payment
- Multi-destination
- Exchange
- Fees
- Accounting accounts

نکته:
Legacy enum قبلی چند مفهوم را مخلوط کرده بود:
- Credit Type
- Block State
- Accounting Account

در معماری جدید این‌ها باید جدا باشند.

---

# 33) Multi-source / Multi-destination

FinancialTransaction فعلی عمداً sourceAccountId/destinationAccountId داخل Aggregate ندارد.

دلیل:
در آینده یک Transaction می‌تواند چند Source و چند Destination داشته باشد.

مثلاً:

Transaction T100
- GENERAL -100
- DONATION -200
- GIFT -50
- SELLER +300
- PLATFORM_FEE +50

در آینده Transaction Leg / Entry Instruction model اضافه می‌شود.

---

# 34) CreditHold Business Decision

CreditHold مستقل است.

یک Transaction می‌تواند چند Hold بسازد.

مثلاً:
Purchase T100
- Hold SHIPPING
- Hold TAX
- Hold GIFT
- Hold FACILITY

Hold باید در آینده support کند:
- multiple simultaneous holds
- consume
- partial consume
- release
- expire
- reference
- reason
- expiry
- version

---

# 35) Hot Account Concurrency

تصمیم آینده:

Normal account:
Optimistic concurrency

Hot account:
ممکن است نیاز به:
- serialized command processing
- account-key ordering
- partitioned queue
- actor model
- Kafka key by account id

داشته باشد.

قرار نیست retry storm روی DB ساخته شود.

---

# 36) Kafka

هنوز پیاده نشده.

Kafka در آینده برای:
- integration events
- reporting
- global accounting projections
- analytics
- cross-service workflows
- read models

استفاده می‌شود.

Kafka Source of Truth اصلی Event Store نیست.

PostgreSQL state/ledger/outbox authoritative هستند.

---

# 37) Redis

هنوز پیاده نشده.

در آینده برای:
- cache
- read optimization
- rate/state helpers

ممکن است استفاده شود.

Balance authoritative در Redis نخواهد بود.

---

# 38) Current Commit / Milestone Status

Milestoneهای بسته‌شده مفهومی:

1. Modular Maven foundation
2. Pure Domain
3. CreditAmount
4. CreditAccount
5. Domain Events
6. Flyway/PostgreSQL
7. CreditHold foundation
8. FinancialTransaction
9. Purchase Application Flow
10. jOOQ persistence
11. Integration test
12. Ledger + Outbox
13. REST Purchase API
14. DB-backed concurrent idempotency
15. ConcurrentPurchaseIntegrationTest

آخرین وضعیت:
همه تست‌های فعلی سبز هستند.

---

# 39) Current Purchase Flow — Final Reference

این Flow الان Reference Implementation پروژه است.

Request:

POST /api/v1/purchases

↓
PurchaseController

↓
PurchaseCommand

↓
PurchaseApplicationService.execute()

↓
Fast idempotency lookup

↓
UnitOfWork

↓
FinancialTransaction.create()

↓
transaction.start()

↓
transactionRepository.save(PROCESSING)
[idempotency DB gate]

↓
load source CreditAccount

↓
load destination CreditAccount

↓
source.debit()

↓
destination.addCredit()

↓
save source

↓
save destination

↓
create balanced LedgerTransaction

↓
append Ledger

↓
transaction.complete()

↓
transactionRepository.save(COMPLETED)

↓
OutboxMessage(PURCHASE_COMPLETED)

↓
COMMIT

اگر duplicate concurrent:
Unique reference constraint
↓
DuplicateKeyException
↓
DuplicateRequestException
↓
Rollback losing transaction
↓
Load winning transaction
↓
Return same result

---

# 40) چیزی که نباید در شروع جلسه بعد دوباره انجام شود

این‌ها DONE هستند و نباید از صفر بازطراحی شوند:

- Maven modules
- Spring Boot runtime
- PostgreSQL connection
- Flyway
- jOOQ generation
- Generated Sources Root
- CreditAccount basic model
- FinancialTransaction basic model
- Purchase v1
- Ledger basic double-entry
- Outbox basic
- UnitOfWork
- Concurrent idempotency test

---

# 41) چیزهایی که هنوز انجام نشده‌اند

این موارد هنوز TODO هستند:

## Platform/Foundation
- apex-platform-core
- apex-platform-events
- EventEnvelope
- IntegrationEvent common contract
- Event metadata
- correlationId
- causationId
- cellId
- eventVersion
- shared serialization

## Kafka
- Outbox publisher
- Kafka producer
- delivery status
- retries
- DLQ
- consumer conventions

## CreditHold Complete Flow
- release
- expire
- persistence repository
- application service
- REST API
- ledger effect
- outbox event
- integration tests

## Error Platform
- common error contract
- centralized business exception mapping

## Read/Reporting
- projections
- ClickHouse / OLAP
- reporting pipeline

## Cell scaling
- Cell Router
- multi-cell deployment
- account routing
- cross-cell saga

## Policies
- Policy Context
- limits
- transfer rules
- allowed credit transitions
- dynamic rule configuration

## Exchange/MQS
- rate
- unit conversion
- asset/unit balancing

## Settlement
- seller
- terminal
- bank settlement

## Accounting
- tax
- fees
- PSP accounts
- revenue accounts

---

# 42) مهم‌ترین تصمیم مرحله بعد

آخرین بحث این بود:

چون سیستم در آینده حداقل 36 Microservice خواهد داشت، قبل از ادامه Business Flowهای زیاد باید یک Foundation مشترک برای Cross-Cutting Concerns ساخته شود.

پیشنهاد اولویت:

## NEXT STEP پیشنهادی

ساخت:

`apex-platform-core`

و:

`apex-platform-events`

ابتدا:
- DomainEvent contract مشترک
- IntegrationEvent
- EventEnvelope<T>
- EventMetadata
- event version
- correlationId
- causationId
- sourceService
- cellId
- occurredAt

سپس Purchase فعلی به عنوان Reference Implementation روی Event Platform جدید migrate شود.

بعد از سبز ماندن Purchase:
CreditHold را end-to-end کامل کنیم.

---

# 43) نکته بسیار مهم برای ادامه

در جلسه بعد نباید دوباره وارد توسعه پراکنده و بسیار ریز شویم.

روش توسعه انتخاب‌شده:
## Vertical Slice Development

هر Flow باید end-to-end بسته شود.

Purchase v1 نمونه کامل است.

Flow بعدی باید مشابه آن کامل شود:

Domain
↓
Application
↓
Persistence
↓
Ledger
↓
Outbox
↓
API
↓
Integration Test
↓
Concurrency/Idempotency where needed

---

# 44) سبک مورد انتظار ادامه توسعه

کاربر ترجیح می‌دهد:
- مراحل ساختاریافته باشند
- فایل‌ها دقیق مشخص شوند
- هر Milestone یک Flow قابل اجرا بسازد
- قبل از معماری پیچیده، یک Reference Flow کامل باشد
- سرعت توسعه بالا باشد
- از کلاس‌های تزئینی غیرضروری پرهیز شود
- Commitهای milestone محور انجام شود

فرمت مطلوب ادامه:

هدف مرحله
↓
فایل‌های جدید/تغییریافته
↓
کد
↓
تست
↓
Commit message
↓
مرحله بعد

---

# 45) اولین پیام پیشنهادی برای چت بعدی

بعد از آپلود این فایل می‌توان گفت:

«این Project Handoff وضعیت فعلی apex-financial-engine است.
همه تست‌های Purchase، Ledger، Outbox و Concurrent Idempotency سبز هستند.
موارد apex-platform که در انتهای فایل آمده هنوز پیاده نشده‌اند.
از بخش NEXT STEP ادامه بده و apex-platform-core + apex-platform-events را طراحی و مرحله‌ای پیاده کنیم، بدون بازطراحی بخش‌های Done.»

---

# END OF HANDOFF
