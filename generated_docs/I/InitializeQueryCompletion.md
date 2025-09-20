# InitializeQueryCompletion

## Location
[src/backend/tcop/cmdtag.c:40-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/cmdtag.c#L40-L46)

## Overview
Initializes a QueryCompletion structure to its default values, setting the command tag to unknown and the processed row count to zero.

## Definition

```c
void
InitializeQueryCompletion(QueryCompletion *qc)
```
## Detailed Description
This function performs basic initialization of a QueryCompletion structure by setting its fields to safe default values. It sets the commandTag field to CMDTAG_UNKNOWN (which displays as "???") and resets the nprocessed counter to 0. This ensures that QueryCompletion structures start in a predictable state before being populated with actual query execution results.

The function is typically called before executing queries to ensure the completion information starts from a clean state, preventing stale data from previous operations from persisting.

## Parameters / Member Variables
- : Pointer to the QueryCompletion structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - QueryCompletion (struct type)
  - CMDTAG_UNKNOWN (enum constant)
- Called from (representative examples):
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md) (src/backend/executor/spi.c:2717)
  - [PortalRun](../P/PortalRun.md) (src/backend/tcop/pquery.c:705)
  - [FillPortalStore](../F/FillPortalStore.md) (src/backend/tcop/pquery.c:1003)
  - CopyQueryCompletion (src/include/tcop/cmdtag.h:52)

## Notes and Other Information
- The QueryCompletion structure contains two fields: commandTag (CommandTag enum) and nprocessed (uint64 counter)
- CMDTAG_UNKNOWN corresponds to the textual representation "???" and has event_trigger_ok=false, table_rewrite_ok=false, rowcount=false
- This initialization is essential for proper query completion tracking in PostgreSQL's command execution pipeline
- The function provides a centralized way to ensure consistent initialization across different parts of the codebase