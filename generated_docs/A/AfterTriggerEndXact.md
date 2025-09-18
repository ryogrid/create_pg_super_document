# AfterTriggerEndXact

## Location
src/backend/commands/trigger.c: 5340 - 5387

## Overview
Cleans up the after-trigger subsystem when a transaction is finishing, canceling any unfired triggers and discarding all pending trigger events.

## Definition


## Detailed Description
AfterTriggerEndXact performs cleanup of the after-trigger subsystem when a transaction is ending, whether through commit or abort. The function discards all pending trigger events since unfired triggers are canceled when a transaction finishes. It safely handles repeated calls during error conditions (such as transaction abort scenarios) and performs memory cleanup by deleting the event context and resetting various trigger-related data structures.

The function operates by:
1. Deleting the pending-events memory context to free up potentially large amounts of memory
2. Resetting the events queue (head, tail, tailfree pointers)
3. Clearing subtransaction state information
4. Resetting query stack and constraint-related state
5. Setting query_depth to -1 to prevent further trigger manipulation until the next transaction

## Parameters / Member Variables
- : Boolean indicating whether the transaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (src/backend/access/transam/xact.c:2255)
  - [PrepareTransaction](../P/PrepareTransaction.md) (src/backend/access/transam/xact.c:2511)
  - [AbortTransaction](AbortTransaction.md) (src/backend/access/transam/xact.c:2856)

## Notes and Other Information
- Can be called repeatedly during error conditions without causing issues
- Memory cleanup is optimized - subtransaction and query state memory is left to be cleaned up by TopTransactionContext reset
- The function prioritizes immediate cleanup of the potentially large pending-events list to free memory quickly
- Sets query_depth to -1 as a safety measure to prevent trigger manipulation until the next transaction begins