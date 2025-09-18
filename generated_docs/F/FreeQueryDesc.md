# FreeQueryDesc

## Location
[src/backend/tcop/pquery.c:105-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L105-L135)

## Overview
FreeQueryDesc properly deallocates a QueryDesc structure and unregisters its associated snapshots to prevent memory leaks and maintain proper resource management.

## Definition
```c
void FreeQueryDesc(QueryDesc *qdesc)
```

## Detailed Description
FreeQueryDesc is the destructor function for QueryDesc structures created by CreateQueryDesc. It performs essential cleanup by unregistering the snapshots that were registered during QueryDesc creation, ensuring proper snapshot reference counting and memory management. The function includes a safety assertion to verify that the QueryDesc is not associated with a live executor state, preventing premature cleanup of active queries. Only the QueryDesc structure itself is freed, as other referenced objects (like PlannedStmt, parameters, etc.) are managed separately and may be shared across multiple contexts.

## Parameters / Member Variables
- `qdesc`: Pointer to the QueryDesc structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - QueryDesc
  - UnregisterSnapshot (called twice)
  - Assert (for safety check)
  - [pfree](../p/pfree.md) (for memory deallocation)
- Called from (representative examples):
  - [ProcessQuery](../P/ProcessQuery.md)
  - [PortalCleanup](../P/PortalCleanup.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [postquel_end](../p/postquel_end.md)

## Notes and Other Information
The function must only be called when the QueryDesc is not actively being used for query execution (estate must be NULL). This is enforced by an assertion check. The snapshots are unregistered rather than simply freed because PostgreSQL uses reference counting for snapshot management. Calling this function on an active QueryDesc will trigger an assertion failure in debug builds.