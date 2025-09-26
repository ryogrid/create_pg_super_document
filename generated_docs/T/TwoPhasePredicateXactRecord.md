# TwoPhasePredicateXactRecord

## Location
[src/include/storage/predicate_internals.h:441-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L441-L445)

## Overview
TwoPhasePredicateXactRecord is a struct that stores per-transaction information needed to reconstruct a SERIALIZABLEXACT during two-phase commit recovery.

## Definition
```c
typedef struct TwoPhasePredicateXactRecord
{
    TransactionId xmin;
    uint32        flags;
} TwoPhasePredicateXactRecord;
```

## Detailed Description
This structure contains the minimal information required to reconstruct serializable transaction state during recovery of prepared transactions in PostgreSQL's two-phase commit protocol. The design is intentionally minimal because most SERIALIZABLEXACT information is not meaningful for a recovered prepared transaction.

Key design principles:
- Does not record in/out conflict lists since associated SERIALIZABLEXACTs will not be available after recovery
- Instead records the existence of conflicts using summary flags
- Focuses only on essential transaction identification and state information

The structure is used during the prepare phase to serialize critical transaction state that must survive a crash and be available during recovery.

## Parameters / Member Variables
- `xmin`: TransactionId representing the minimum transaction ID visible to this serializable transaction
- `flags`: uint32 bitmask containing summary conflict information and other transaction state flags

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (PostgreSQL transaction identifier type)
- Called from (representative examples):
  - [AtPrepare_PredicateLocks](../A/AtPrepare_PredicateLocks.md)
  - [predicatelock_twophase_recover](../p/predicatelock_twophase_recover.md)
  - [TwoPhasePredicateRecord](TwoPhasePredicateRecord.md)

## Notes and Other Information
- Located in src/include/storage/predicate_internals.h:441-445
- Part of PostgreSQL's serializable snapshot isolation implementation
- Critical for maintaining ACID properties across two-phase commit boundaries
- The simplified design reflects the reality that full conflict tracking cannot be reconstructed after recovery