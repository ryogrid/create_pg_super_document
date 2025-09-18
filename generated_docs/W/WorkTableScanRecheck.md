# WorkTableScanRecheck

## Location
[src/backend/executor/nodeWorktablescan.c:66-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWorktablescan.c#L66-L80)

## Overview
WorkTableScanRecheck is an access method routine used during EvalPlanQual processing to recheck tuples for worktable scans, though it currently requires no actual checking.

## Definition
static bool WorkTableScanRecheck(WorkTableScanState *node, TupleTableSlot *slot)

## Detailed Description
WorkTableScanRecheck implements the recheck interface required for EvalPlanQual (EPQ) processing in PostgreSQL's executor. EvalPlanQual is used to handle concurrent updates during query execution by rechecking whether tuples still satisfy query conditions after potential modifications by other transactions. However, for worktable scans, no actual rechecking is necessary because worktables contain temporary data that is not subject to concurrent modifications from other transactions, making the recheck operation trivial.

## Parameters / Member Variables
- `node`: WorkTableScanState pointer containing the scan state (currently unused)
- `slot`: TupleTableSlot containing the tuple to recheck (currently unused)

## Dependencies
- Types used:
  - [WorkTableScanState](WorkTableScanState.md) (scan state structure)
  - TupleTableSlot (tuple storage slot)
- Called from:
  - [ExecWorkTableScan](../E/ExecWorkTableScan.md) (as part of the access method interface)

## Notes and Other Information
- Always returns true since no actual checking is required for worktables
- Part of the standard scan access method interface for EvalPlanQual support
- Worktables are temporary and not subject to concurrent modifications
- Parameters are currently unused but maintained for interface consistency
- Essential for maintaining the executor's standard access method pattern