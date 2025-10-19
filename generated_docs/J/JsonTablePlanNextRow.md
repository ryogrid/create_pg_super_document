# JsonTablePlanNextRow

## Location
[src/backend/utils/adt/jsonpath_exec.c:4293-4319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4293-L4319)

## Overview
JsonTablePlanNextRow is a dispatcher function that fetches the next row from a JSON table plan by delegating to the appropriate plan-specific function based on the plan type.

## Definition

```c
static bool
JsonTablePlanNextRow(JsonTablePlanState *planstate)
```
## Detailed Description
This function serves as a polymorphic dispatcher for different types of JSON table plans. It examines the plan type stored in the planstate and calls the appropriate specialized function to advance to the next row. The function supports two main plan types: JsonTablePathScan for scanning operations and JsonTableSiblingJoin for join operations. If an invalid plan type is encountered, the function raises an error.

## Parameters / Member Variables
- `*planstate`: Pointer to JsonTablePlanState structure containing the current state of the JSON table plan execution, including the plan type and associated data
## Dependencies
- Functions called/Symbols referenced:
  - [JsonTablePlanScanNextRow](JsonTablePlanScanNextRow.md) (for JsonTablePathScan plans)
  - [JsonTablePlanJoinNextRow](JsonTablePlanJoinNextRow.md) (for JsonTableSiblingJoin plans)
  - IsA (PostgreSQL macro for type checking)
  - elog (PostgreSQL error logging function)
- Called from (representative examples):
  - [JsonTablePlanScanNextRow](JsonTablePlanScanNextRow.md) (for nested plan execution)
  - [JsonTablePlanJoinNextRow](JsonTablePlanJoinNextRow.md) (for join plan execution) 
  - [JsonTableFetchRow](JsonTableFetchRow.md) (main row fetching interface)

## Notes and Other Information
- This function implements a simple visitor pattern for different JSON table plan types
- Returns true if a row was successfully retrieved, false if the plan has exhausted all rows
- The function includes an assertion and compiler appeasement return statement that should never be reached
- [Plan](../P/Plan.md) type validation is performed at runtime, with invalid types triggering an ERROR-level log message

## Simplified Source

```c
static bool JsonTablePlanNextRow(JsonTablePlanState *planstate) {
    // Dispatch to appropriate plan-specific function based on plan type
    if (IsA(planstate->plan, JsonTablePathScan))
        return JsonTablePlanScanNextRow(planstate);
    else if (IsA(planstate->plan, JsonTableSiblingJoin))
        return JsonTablePlanJoinNextRow(planstate);
    else
        elog(ERROR, "invalid JsonTablePlan %d", (int) planstate->plan->type);

    return false; // Should never reach here
}
```