# simple_heap_delete

## Location
src/backend/access/heap/heapam.c: 3154 - 3199

## Overview
simple_heap_delete is a simplified wrapper around heap_delete that provides a convenient interface for deleting tuples when concurrent updates are not expected and any failures should result in errors.

## Definition


## Detailed Description
simple_heap_delete serves as a high-level wrapper around the more complex heap_delete function, designed for use cases where the caller expects the deletion to succeed unconditionally. This function is typically used when the caller holds appropriate locks on the relation that prevent concurrent modifications, making failure conditions unexpected.

The function internally calls heap_delete with preset parameters:
- Uses the current command ID
- No crosscheck snapshot validation
- Always waits for concurrent transactions
- Not part of a partition move operation

Upon receiving the result from heap_delete, it translates all non-success outcomes into ERROR conditions, making this function suitable for scenarios where any form of failure should abort the current transaction. This design pattern is common in PostgreSQL's catalog manipulation functions where system consistency requires that operations either succeed completely or fail with clear error reporting.

## Parameters / Member Variables
- : The heap relation containing the tuple to delete
- : ItemPointer identifying the specific tuple location (page and offset)

## Dependencies
- Functions called/Symbols referenced:
  - heap_delete
  - GetCurrentCommandId
  - TM_Result enum values (TM_Ok, TM_SelfModified, TM_Updated, TM_Deleted)
- Called from (representative examples):
  - toast_delete_datum
  - CatalogTupleDelete

## Notes and Other Information
- This function assumes the caller has appropriate locking to prevent concurrent modifications
- All failure conditions result in ereport(ERROR), aborting the current transaction
- Commonly used in catalog operations where tuple deletion must succeed or the entire operation should fail
- The function provides no mechanism for graceful handling of concurrent modification scenarios
- Uses InvalidSnapshot for crosscheck, meaning no additional snapshot-based validation is performed
- Always sets wait=true, meaning it will wait for concurrent transactions rather than failing immediately