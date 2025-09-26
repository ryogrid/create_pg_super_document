# TwoPhasePredicateLockRecord

## Location
[src/include/storage/predicate_internals.h:448-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L448-L452)

## Overview
TwoPhasePredicateLockRecord is a struct that stores per-lock state information for serializable predicate locks during two-phase commit operations.

## Definition
```c
typedef struct TwoPhasePredicateLockRecord
{
    PREDICATELOCKTARGETTAG target;
    uint32                 filler;    /* to avoid length change in back-patched fix */
} TwoPhasePredicateLockRecord;
```

## Detailed Description
This structure represents the essential state information for individual predicate locks that must be preserved during two-phase commit operations. It contains the target identification for the lock and maintains structure compatibility for back-patching.

The structure is designed to:
- Identify the specific target of a predicate lock
- Maintain binary compatibility across PostgreSQL versions through the filler field
- Enable reconstruction of predicate lock state during recovery after a prepared transaction

Predicate locks are crucial for maintaining serializable isolation by preventing phantom reads and ensuring that concurrent transactions cannot create conflicts that would violate serializability.

## Parameters / Member Variables
- `target`: PREDICATELOCKTARGETTAG identifying the specific target (relation, page, or tuple) that this predicate lock protects
- `filler`: uint32 padding field included to avoid structure size changes in back-patched fixes, ensuring binary compatibility

## Dependencies
- Functions called/Symbols referenced:
  - [PREDICATELOCKTARGETTAG](../P/PREDICATELOCKTARGETTAG.md) (identifies lock targets)
- Called from (representative examples):
  - [AtPrepare_PredicateLocks](../A/AtPrepare_PredicateLocks.md)
  - [predicatelock_twophase_recover](../p/predicatelock_twophase_recover.md)
  - [TwoPhasePredicateRecord](TwoPhasePredicateRecord.md)

## Notes and Other Information
- Located in src/include/storage/predicate_internals.h:448-452
- Part of PostgreSQL's serializable snapshot isolation implementation
- The filler field demonstrates careful attention to maintaining ABI stability across patch releases
- Works in conjunction with TwoPhasePredicateXactRecord to preserve complete predicate lock state
- Essential for preventing serialization anomalies in prepared transactions