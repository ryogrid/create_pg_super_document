# AtEOXact_Enum

## Location
[src/backend/catalog/pg_enum.c:726-760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L726-L760)

## Overview
Performs cleanup of enum-related data structures at the end of a top-level transaction, resetting the tables that track uncommitted enum types and values.

## Definition

```c
enum_types = NULL;
```
## Detailed Description
This function is called at the end of transaction processing (both commit and abort) to clean up the enum subsystem's transaction-local state. It resets the global pointers to the hash tables that track uncommitted enum types and values ( and ) back to NULL.

The function relies on PostgreSQL's memory context system for actual memory cleanup - since these hash tables are allocated in TopTransactionContext, their memory will be automatically freed when the transaction context is destroyed. The function simply needs to reset the global pointers to ensure they don't point to stale data in subsequent transactions.

This cleanup ensures that the next transaction starts with a clean slate regarding enum tracking, preventing any interference between transactions.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - uncommitted_enum_types: Global pointer to hash table tracking uncommitted enum types
  - uncommitted_enum_values: Global pointer to hash table tracking uncommitted enum values
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md): Called during transaction commit to clean up enum state
  - [PrepareTransaction](../P/PrepareTransaction.md): Called during two-phase commit preparation
  - [AbortTransaction](AbortTransaction.md): Called during transaction abort to clean up enum state

## Notes and Other Information
- This is a transaction lifecycle callback function, part of PostgreSQL's end-of-transaction cleanup mechanism
- The function is called for both successful commits and aborted transactions
- Memory cleanup is handled automatically by the memory context system - this function only resets pointers
- Essential for preventing memory leaks and ensuring proper isolation between transactions
- The 'AtEOXact' prefix follows PostgreSQL's naming convention for end-of-transaction callback functions
- Simple but critical function that ensures enum subsystem state doesn't leak between transactions