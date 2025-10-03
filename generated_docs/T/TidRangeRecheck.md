# TidRangeRecheck

## Location
[src/backend/executor/nodeTidrangescan.c:273-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidrangescan.c#L273-L293)

## Overview
TidRangeRecheck is an access method routine used to recheck a tuple during EvalPlanQual processing in TID range scans.

## Definition

```c
static bool
TidRangeRecheck(TidRangeScanState *node, TupleTableSlot *slot)
```
## Detailed Description
This function serves as the recheck routine for TID range scans during EvalPlanQual (EPQ) processing. EvalPlanQual is PostgreSQL's mechanism for handling concurrent updates in READ COMMITTED isolation level transactions. When a tuple needs to be rechecked due to concurrent modifications, this function is called. Currently, the implementation simply returns true, indicating that all tuples retrieved by TID range scans are considered valid during recheck operations. This simplified approach is appropriate because TID-based scans directly target specific physical tuple locations.

## Parameters / Member Variables
- `node`: TidRangeScanState containing the scan state information
- `slot`: TupleTableSlot containing the tuple to be rechecked

## Dependencies
- Data structures used:
  - [TidRangeScanState](TidRangeScanState.md)
  - [TupleTableSlot](TupleTableSlot.md)
- Called from:
  - [ExecTidRangeScan](../E/ExecTidRangeScan.md) (as part of EPQ processing)

## Notes and Other Information
- Always returns true, indicating successful recheck
- This simplified implementation assumes TID range scans don't require complex recheck logic
- Part of the executor node interface for handling concurrent transaction scenarios
- The function signature matches the standard recheck interface used by other scan types
- Used specifically during EvalPlanQual processing when tuple visibility needs verification

## Simplified Source

```c
static bool
TidRangeRecheck(TidRangeScanState *node, TupleTableSlot *slot)
{
    // TID range scans don't require complex recheck logic
    return true;
}
```