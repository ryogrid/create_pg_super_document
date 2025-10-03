# TableFuncRecheck

## Location
[src/backend/executor/nodeTableFuncscan.c:81-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L81-L96)

## Overview
TableFuncRecheck is a static access method routine used during EvalPlanQual operations to recheck tuple validity, though for table function scans it always returns true as no rechecking is necessary.

## Definition

```c
static bool
TableFuncRecheck(TableFuncScanState *node, TupleTableSlot *slot)
```
## Detailed Description
TableFuncRecheck implements the recheck interface required by PostgreSQL's EvalPlanQual mechanism, which is used during concurrent transaction processing to ensure tuple visibility and consistency. However, since table functions generate deterministic results that are not subject to concurrent modifications by other transactions, this function simply returns true without performing any actual checks.

The function serves as a placeholder implementation of the recheck interface, maintaining consistency with PostgreSQL's scan node architecture while acknowledging that table function results don't require rechecking.

## Parameters / Member Variables
- `*node`: TableFuncScanState structure containing the scan state (unused in this implementation)
- `*slot`: TupleTableSlot containing the tuple to recheck (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [TableFuncScanState](TableFuncScanState.md) (struct type)
- Called from:
  - [ExecTableFuncScan](../E/ExecTableFuncScan.md)

## Notes and Other Information
- Always returns true since table function results are immutable and don't require rechecking
- Part of the EvalPlanQual framework for handling concurrent transaction scenarios
- The function is static, indicating it's only used within the nodeTableFuncscan.c file
- Both parameters are effectively unused since no actual rechecking logic is needed for table functions

## Simplified Source

```c
static bool
TableFuncRecheck(TableFuncScanState *node, TupleTableSlot *slot)
{
    // Table function results are immutable - no rechecking needed
    return true;
}
```