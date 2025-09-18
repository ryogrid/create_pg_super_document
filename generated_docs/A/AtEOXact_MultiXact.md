# AtEOXact_MultiXact

## Location
[src/backend/access/transam/multixact.c:1800-1827](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1800-L1827)

## Overview
Handles cleanup of MultiXact-related state at the end of a transaction (both commit and abort).

## Definition
```c
void AtEOXact_MultiXact(void)
```

## Detailed Description
This function is called at the end of every top-level transaction, regardless of whether it commits or aborts. It performs essential cleanup of MultiXact-related state to ensure proper isolation and resource management. The function resets the process-local oldest MultiXact ID tracking variables and discards the local MultiXact cache. The cache cleanup is automatic since MXactContext was created as a child of TopTransactionContext and will be destroyed when the transaction context is reset.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - InvalidMultiXactId (constant)
  - [dclist_init](../d/dclist_init.md)
- Global variables modified:
  - OldestMemberMXactId[MyProcNumber]
  - OldestVisibleMXactId[MyProcNumber]
  - MXactContext
  - MXactCache
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [AbortTransaction](AbortTransaction.md)

## Notes and Other Information
- Called at both transaction commit and abort - [cleanup](../c/cleanup.md) is identical for both cases
- Assumes that storing a MultiXactId is atomic, so no locking is required
- The MultiXact cache is automatically cleaned up due to memory context hierarchy
- Essential for maintaining proper MultiXact visibility and preventing resource leaks
- Part of the transaction cleanup protocol in PostgreSQL
- Located in src/backend/access/transam/multixact.c:1800-1827