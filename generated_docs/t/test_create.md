# test_create

## Location
src/test/modules/test_tidstore/test_tidstore.c: 86 - 135

## Overview
A PostgreSQL C function that creates and initializes a TidStore for testing purposes, with support for both local and shared memory configurations.

## Definition


## Detailed Description
This function creates a TidStore (Tuple Identifier Store) for testing the tidstore functionality. It supports two creation modes:
- **Shared mode**: Creates the tidstore using Dynamic Shared Area (DSA) for inter-process communication
- **Local mode**: Creates the tidstore in local memory context

The function initializes several key components:
1. Creates the TidStore with a maximum size hint of 2MB
2. Sets up arrays for storing test ItemPointers (insert_tids, lookup_tids, iter_tids) with initial capacity of 256 items each
3. Records the empty tidstore size for later memory usage calculations
4. For shared tidstores, registers a new LWLock tranche and pins the DSA mapping

The tidstore is created in TopMemoryContext to persist across multiple test function calls within the same backend process.

## Parameters / Member Variables
- Function takes one boolean argument via PG_GETARG_BOOL(0):
  - : If true, creates a shared tidstore using DSA; if false, creates a local tidstore

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - LWLockNewTrancheId
  - LWLockRegisterTranche
  - [TidStoreCreateShared](../T/TidStoreCreateShared.md)
  - [TidStoreGetDSA](../T/TidStoreGetDSA.md)
  - [dsa_pin_mapping](../d/dsa_pin_mapping.md)
  - [TidStoreCreateLocal](../T/TidStoreCreateLocal.md)
  - [TidStoreMemoryUsage](../T/TidStoreMemoryUsage.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - This is a SQL-callable function, typically invoked from test scripts

## Notes and Other Information
- This is a PostgreSQL extension function exposed to SQL for testing purposes
- The function uses global variables (tidstore, items) to maintain state across test calls
- The shared tidstore uses DSA but is still only usable by the same process that created it
- The function includes memory context switching to ensure proper memory management
- Uses VACUUM's opposite configuration (insert_exact=false) for broader test coverage
- The function will assert if called when tidstore is already initialized