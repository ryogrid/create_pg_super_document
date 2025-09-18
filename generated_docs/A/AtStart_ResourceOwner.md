# AtStart_ResourceOwner

## Location
[src/backend/access/transam/xact.c:1220-1247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1220-L1247)

## Overview
AtStart_ResourceOwner is a static function that creates and initializes the resource owner for a new transaction, establishing the resource management infrastructure needed to track and clean up transaction resources.

## Definition
```c
static void AtStart_ResourceOwner(void)
```

## Detailed Description
AtStart_ResourceOwner is responsible for setting up the resource ownership hierarchy for a new transaction. A resource owner is PostgreSQL's mechanism for tracking and managing various system resources (such as locks, file descriptors, memory allocations, etc.) that are acquired during transaction execution.

The function creates a top-level resource owner with no parent (NULL parent) and assigns it to multiple global variables to establish the resource ownership context:
- TopTransactionResourceOwner: The root resource owner for the entire transaction
- CurTransactionResourceOwner: The current transaction-level resource owner  
- CurrentResourceOwner: The currently active resource owner

This ensures proper resource tracking and cleanup when the transaction ends, either through commit or abort.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (at line 1222)
  - ResourceOwnerCreate (at line 1232)
  - CurrentTransactionState (global variable)
- Called from (representative examples):
  - [StartTransaction](../S/StartTransaction.md) (src/backend/access/transam/xact.c:2105)

## Notes and Other Information
- This is a static function, only accessible within xact.c
- The resource owner is created with NULL parent, making it a top-level owner
- The "TopTransaction" name helps identify this resource owner in debugging
- Resource owners form a hierarchy that enables proper cleanup on transaction abort
- Critical for preventing resource leaks in PostgreSQL's resource management system
- All transaction-related resources will be owned by this resource owner or its children