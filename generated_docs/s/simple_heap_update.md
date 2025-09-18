# simple_heap_update

## Location
src/backend/access/heap/heapam.c: 4444 - 4484

## Overview
simple_heap_update is a wrapper function that provides a simplified interface for updating heap tuples when concurrent updates are not expected, automatically handling errors through ereport().

## Definition


## Detailed Description
This function serves as a convenience wrapper around heap_update for cases where the caller expects the update to succeed without concurrency conflicts. It's typically used when the relation has an exclusive lock or when concurrent modifications are otherwise prevented. The function calls heap_update with standard parameters and converts any failure conditions into ERROR-level reports, making error handling automatic for the caller.

The function handles all possible heap_update results:
- TM_Ok: Update succeeded (normal case)
- TM_SelfModified: Tuple already modified in current command (error)
- TM_Updated: Concurrent update detected (error)
- TM_Deleted: Tuple was concurrently deleted (error)

## Parameters / Member Variables
- : Relation containing the tuple to update
- : ItemPointer identifying the old tuple's location
- : New heap tuple to replace the old one
- : Control parameter for index update behavior

## Dependencies
- Functions called/Symbols referenced:
  - [heap_update](../h/heap_update.md) (core update function)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md) (get current command ID)
  - elog (error reporting)
- Type references:
  - TM_Result (tuple manager result codes)
  - TM_FailureData (failure information structure)
  - [LockTupleMode](../L/LockTupleMode.md) (tuple locking modes)
  - TU_UpdateIndexes (index update control)
- Called from (representative examples):
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog tuple updates)
  - [CatalogTupleUpdateWithInfo](../C/CatalogTupleUpdateWithInfo.md) (catalog updates with additional info)

## Notes and Other Information
- Designed for scenarios where concurrent updates are not expected
- Uses InvalidSnapshot and waits for commit completion
- Automatically reports all failure conditions as ERROR level
- Commonly used for catalog updates and system table modifications
- Part of PostgreSQL's heap access method providing simplified update interface
- Does not return failure codes - either succeeds or throws an error