# AfterTriggerPendingOnRel

## Location
src/backend/commands/trigger.c: 6061 - 6147

## Overview
Tests whether there are any pending after-trigger events for a specified relation, used by DDL operations like TRUNCATE, CLUSTER, and ALTER TABLE to detect if major structural changes would be unsafe.

## Definition


## Detailed Description
This function is a safety mechanism that prevents DDL operations from proceeding when there are unprocessed after-trigger events for a relation. It performs a comprehensive scan of both committed and queued trigger events to determine if any are pending for the specified relation.

The function serves as a critical safety check for operations that perform "major surgery" on relations. Since after-triggers are deferred until transaction commit, operations like TRUNCATE or ALTER TABLE could interfere with pending trigger execution if allowed to proceed. The function examines only local pending events, operating under the assumption that having an exclusive lock on a relation guarantees no unserviced events exist in other backends.

The scan process involves two phases:
1. Scanning the main event queue (afterTriggers.events) for any non-completed events
2. Scanning events queued by incomplete queries across all query depth levels

Events marked with AFTER_TRIGGER_DONE are safely ignored, as even if such flags are rolled back by subxact abort, the effects of the DDL operation would also be rolled back.

## Parameters / Member Variables
- : The OID of the relation to check for pending after-trigger events

## Dependencies
- Functions called/Symbols referenced:
  - for_each_event_chunk (macro for iterating through event chunks)
  - GetTriggerSharedData (retrieves shared trigger data from event)
  - AFTER_TRIGGER_DONE (flag indicating completed trigger events)
- Called from (representative examples):
  - CheckTableNotInUse (in tablecmds.c for DDL safety checks)

## Notes and Other Information
- The function only examines local pending events, relying on exclusive locking to ensure no cross-backend conflicts
- Handles the edge case where TRUNCATE/DDL operations are executed within functions or triggers of updating queries on the same relation
- Returns immediately upon finding any pending event for the relation, making it efficient for the common case where no conflicts exist
- The design acknowledges that removing pending events would require deep knowledge of trigger semantics, so it opts for prevention rather than resolution