# JsonTablePlanScanNextRow

## Location
[src/backend/utils/adt/jsonpath_exec.c:4320-4381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4320-L4381)

## Overview
JsonTablePlanScanNextRow fetches the next row from a JSON table path scan plan, handling nested path evaluations and implementing an outer join pattern for hierarchical JSON data.

## Definition

```c
static bool
JsonTablePlanScanNextRow(JsonTablePlanState *planstate)
```
## Detailed Description
This function implements the core row iteration logic for JsonTablePathScan plans. It manages a two-level iteration process: first checking for rows from nested plans (child paths), then advancing to the next row from the current plan's path evaluation results. The function implements an outer join semantic where if a nested path has no matching rows, the columns at that level will compute to NULL. The function maintains the current row state in the planstate structure and manages memory contexts appropriately to ensure proper cleanup.

## Parameters / Member Variables
- `*planstate`: Pointer to JsonTablePlanState structure containing the current scan state, including the found values list, iterator position, current row data, nested plans, and memory context
## Dependencies
- Functions called/Symbols referenced:
  - [JsonTablePlanNextRow](JsonTablePlanNextRow.md) (recursive call for nested plans)
  - [JsonValueListNext](JsonValueListNext.md) (advances through the list of found JSON values)
  - [JsonbValueToJsonb](JsonbValueToJsonb.md) (converts JsonbValue to Jsonb format)
  - [JsonbPGetDatum](JsonbPGetDatum.md) (creates a PostgreSQL Datum from Jsonb)
  - [JsonTableResetNestedPlan](JsonTableResetNestedPlan.md) (resets nested plans for re-evaluation)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (manages memory contexts)
  - [PointerGetDatum](../P/PointerGetDatum.md) (creates NULL datum)
- Called from (representative examples):
  - [JsonTablePlanNextRow](JsonTablePlanNextRow.md) (dispatcher function for different plan types)

## Notes and Other Information
- Returns true if more rows are available (either from current plan or nested plans), false when exhausted
- Implements outer join semantics for nested JSON paths
- Manages ordinal numbering for row sequencing (planstate->ordinal++)
- Uses memory context switching to ensure proper memory management for row data
- The nested plan processing involves a reset-and-advance pattern to maintain hierarchical relationships
- The function handles both cases: when there are active nested rows to join and when new parent rows need to be fetched