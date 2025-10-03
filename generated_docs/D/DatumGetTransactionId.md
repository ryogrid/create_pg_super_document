# DatumGetTransactionId

## Location
[src/include/postgres.h:262-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L262-L271)

## Overview
DatumGetTransactionId is a static inline function that extracts a transaction identifier (TransactionId) value from a Datum, serving as a type conversion utility for PostgreSQL's transaction management system.

## Definition
static inline TransactionId DatumGetTransactionId(Datum X)

## Detailed Description
DatumGetTransactionId performs a simple type cast from a Datum to a TransactionId. This function is part of PostgreSQL's datum conversion interface, providing a consistent method for extracting transaction identifier values from the generic Datum representation. Transaction identifiers are crucial for PostgreSQL's MVCC (Multi-Version Concurrency Control) system, tracking when transactions begin and commit. The function performs no validation or transformation - it simply casts the input Datum directly to a TransactionId type, which is typically a 32-bit unsigned integer.

## Parameters / Member Variables
- X: A Datum value that contains a transaction identifier to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - (None - performs direct cast)
- Called from (representative examples):
  - [ExecCheckTupleVisible](../E/ExecCheckTupleVisible.md) (tuple visibility checking in modify operations)
  - [ExecOnConflictUpdate](../E/ExecOnConflictUpdate.md) (handling conflicts during updates)
  - SLOTSYNC_COLUMN_COUNT (replication slot synchronization)
  - PG_GETARG_TRANSACTIONID (function argument extraction macro)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it available throughout the codebase
- Used primarily in transaction visibility checks and MVCC-related operations
- Part of the family of DatumGet* conversion functions that provide type-safe extraction from Datum values
- TransactionId is a fundamental type in PostgreSQL's concurrency control system
- The function assumes the input Datum actually contains a valid TransactionId value - no type checking is performed
- Commonly used in conjunction with PG_GETARG_TRANSACTIONID macro for extracting transaction ID arguments from PostgreSQL functions
- Essential for operations that need to work with transaction timestamps and visibility information

## Simplified Source

```c
static inline TransactionId DatumGetTransactionId(Datum X) {
    // Simple cast from generic Datum to TransactionId type
    // No validation - assumes input contains valid transaction ID
    return (TransactionId) X;
}
```