# TwoPhasePredicateRecord

## Location
[src/include/storage/predicate_internals.h:454-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L454-L462)

## Overview
TwoPhasePredicateRecord is a union structure that serves as a container for different types of predicate lock information stored in two-phase commit state files.

## Definition
```c
typedef struct TwoPhasePredicateRecord
{
    TwoPhasePredicateRecordType type;
    union
    {
        TwoPhasePredicateXactRecord xactRecord;
        TwoPhasePredicateLockRecord lockRecord;
    }                   data;
} TwoPhasePredicateRecord;
```

## Detailed Description
TwoPhasePredicateRecord is the primary data structure used to serialize predicate lock state during PostgreSQL's two-phase commit operations. It acts as a discriminated union that can hold either transaction-level information or individual predicate lock details.

The structure design follows these principles:
- Uses a type discriminator to identify which union member is active
- Supports two distinct record types: transaction records and lock records
- Enables efficient serialization of heterogeneous predicate lock data
- Facilitates recovery of serializable transaction state after crashes

During the prepare phase of two-phase commit, the system generates one per-transaction record and a variable number of per-predicate-lock records, all stored using this unified structure.

## Parameters / Member Variables
- `type`: TwoPhasePredicateRecordType enum value indicating the record type:
  - `TWOPHASEPREDICATERECORD_XACT`: Contains transaction-level information
  - `TWOPHASEPREDICATERECORD_LOCK`: Contains individual predicate lock information
- `data`: Union containing the actual record data:
  - `xactRecord`: TwoPhasePredicateXactRecord containing per-transaction information for SERIALIZABLEXACT reconstruction
  - `lockRecord`: TwoPhasePredicateLockRecord containing per-lock state information

## Dependencies
- Functions called/Symbols referenced:
  - TwoPhasePredicateRecordType (discriminator enum)
  - [TwoPhasePredicateXactRecord](TwoPhasePredicateXactRecord.md) (transaction record type)
  - [TwoPhasePredicateLockRecord](TwoPhasePredicateLockRecord.md) (lock record type)
- Called from (representative examples):
  - [AtPrepare_PredicateLocks](../A/AtPrepare_PredicateLocks.md)
  - [predicatelock_twophase_recover](../p/predicatelock_twophase_recover.md)

## Notes and Other Information
- Located in src/include/storage/predicate_internals.h:454-462
- Core component of PostgreSQL's serializable isolation implementation
- Enables persistence of predicate lock state across two-phase commit boundaries
- The discriminated union design provides type safety and efficient storage
- Critical for maintaining ACID properties in distributed transactions involving serializable isolation