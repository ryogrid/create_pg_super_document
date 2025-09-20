# ForeignRecheck

## Location
[src/backend/executor/nodeForeignscan.c:78-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L78-L117)

## Overview
ForeignRecheck is a static function that validates whether a tuple still meets the qualification conditions during EvalPlanQual processing for foreign scan operations.

## Definition

```c
static bool
ForeignRecheck(ForeignScanState *node, TupleTableSlot *slot)
```
## Detailed Description
ForeignRecheck serves as the access method routine for rechecking tuples during EvalPlanQual processing in foreign scans. EvalPlanQual is PostgreSQL's mechanism for handling concurrent updates in READ COMMITTED isolation level, requiring re-evaluation of tuples that may have been modified by other transactions.

The function operates in two phases:
1. **FDW-specific recheck**: If the FDW provides a RecheckForeignScan callback, it is invoked to perform FDW-specific validation. This is particularly important for outer joins that have been pushed down to the foreign server, where different columns may become NULL upon recheck.
2. **Local qualification check**: After the FDW-specific recheck (or if no FDW callback is provided), the function evaluates the fdw_recheck_quals expressions against the tuple to determine if it still satisfies the local qualification conditions.

The expression context is properly set up with the tuple as the scan tuple and reset before qualification evaluation to ensure clean state. The function provides flexibility for FDWs to either handle complex rechecking logic through the callback or rely on the simpler fdw_recheck_quals approach for basic cases.

## Parameters / Member Variables
- : ForeignScanState structure containing the execution state, FDW routines, and recheck qualifications for the foreign scan operation
- : TupleTableSlot containing the tuple to be rechecked for qualification compliance

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - ExecQual
  - RecheckForeignScan (via fdwroutine callback)
- Called from:
  - [ExecForeignScan](../E/ExecForeignScan.md)

## Notes and Other Information
- This function is only called during EvalPlanQual processing when tuples need to be re-evaluated
- The FDW callback RecheckForeignScan is optional; FDWs can choose to rely solely on fdw_recheck_quals
- Outer joins pushed down to foreign servers require special handling as column nullability may change during recheck
- The function returns false if either the FDW-specific recheck fails or the local qualifications are not satisfied
- Expression context cleanup is handled through ResetExprContext to prevent memory leaks during repeated evaluations