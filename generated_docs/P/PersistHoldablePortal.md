# PersistHoldablePortal

## Location
src/backend/commands/portalcmds.c: 316 - 496

## Overview
PersistHoldablePortal prepares a holdable portal for access outside of the current transaction by materializing the entire result set into a tuplestore and shutting down the executor.

## Definition
```c
void PersistHoldablePortal(Portal portal)
```

## Detailed Description
PersistHoldablePortal is responsible for converting a holdable portal from an active executor-based state to a persistent tuplestore-based state that can survive transaction boundaries. This function is called when a holdable cursor needs to persist beyond the transaction that created it. The function executes the entire query (or remaining results), stores all tuples in a tuplestore, and then shuts down the executor to free transaction-specific resources.

The function performs these critical operations:
1. Validates that the portal is properly set up for holdable persistence with required contexts and tuplestore
2. Copies the tuple descriptor to long-term memory (holdContext) since the original was in executor memory
3. Sets up global portal context and activates the portal for execution
4. Handles different execution strategies for scrollable vs non-scrollable cursors
5. Configures the query destination to output directly to the tuplestore with detoasting enabled
6. Executes the query to completion, storing all results in the tuplestore
7. Properly positions the tuplestore cursor based on the portal's current position
8. Shuts down the executor and cleans up execution-related resources
9. Includes comprehensive error handling to mark the portal as failed if errors occur

The function distinguishes between scrollable and non-scrollable cursors: scrollable cursors require the entire result set to be stored from the beginning, while non-scrollable cursors only store remaining unread tuples for efficiency.

## Parameters / Member Variables
- `portal`: Portal pointer to the holdable portal that needs to be persisted

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - MarkPortalActive
  - MarkPortalFailed
  - PushActiveSnapshot
  - PopActiveSnapshot
  - [ExecutorRewind](../E/ExecutorRewind.md)
  - [ExecutorRun](../E/ExecutorRun.md)
  - [ExecutorFinish](../E/ExecutorFinish.md)
  - [ExecutorEnd](../E/ExecutorEnd.md)
  - [FreeQueryDesc](../F/FreeQueryDesc.md)
  - [CreateDestReceiver](../C/CreateDestReceiver.md)
  - [SetTuplestoreDestReceiverParams](../S/SetTuplestoreDestReceiverParams.md)
  - [tuplestore_skiptuples](../t/tuplestore_skiptuples.md)
  - [tuplestore_rescan](../t/tuplestore_rescan.md)
  - [MemoryContextDeleteChildren](../M/MemoryContextDeleteChildren.md)
- Called from (representative examples):
  - [Portal](Portal.md) persistence mechanisms during transaction commit for holdable cursors

## Notes and Other Information
- This function must be called within the transaction that originally created the portal (createSubid validation)
- The tuplestore receiver is configured to detoast all data, making it safe to not keep a snapshot with the data
- For scrollable cursors, ExecutorRewind is called to restart from the beginning before materializing the entire result set
- For non-scrollable cursors, only remaining tuples are stored, and positioning logic accounts for the offset
- The function uses PG_TRY/PG_CATCH for comprehensive error handling, ensuring proper cleanup and portal state marking
- After successful completion, the portal can only be accessed via the tuplestore, not through executor calls
- All subsidiary memory contexts are cleaned up after persistence to free transaction-specific resources
- The holdContext and holdStore must be pre-allocated by the caller before calling this function