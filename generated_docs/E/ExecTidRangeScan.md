# ExecTidRangeScan

## Location
src/backend/executor/nodeTidrangescan.c: 294 - 307

## Overview
ExecTidRangeScan is the main execution function for TID range scan operations, responsible for scanning a relation using tuple identifiers (TIDs) and returning the next qualifying tuple from the range.

## Definition
```c
static TupleTableSlot *ExecTidRangeScan(PlanState *pstate)
```

## Detailed Description
ExecTidRangeScan serves as the primary execution routine for TID range scans in PostgreSQL's executor. It implements a wrapper around the generic ExecScan infrastructure, providing the appropriate access method functions specific to TID range scanning. The function operates under the assumption that the "cursor" maintained by the access method interface (AMI) is positioned at the previously returned tuple, and that the relation is already opened for TID range scanning.

The function delegates the actual scanning work to ExecScan, passing TidRangeNext as the access method function and TidRangeRecheck as the recheck method. This design follows PostgreSQL's standard executor pattern where specific scan types provide their access methods to the generic scanning framework.

## Parameters / Member Variables
- `pstate`: A PlanState pointer that is cast to TidRangeScanState, containing the execution state and context for the TID range scan operation

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for casting PlanState to TidRangeScanState)
  - [ExecScan](ExecScan.md) (generic scan execution framework)
  - [TidRangeNext](../T/TidRangeNext.md) (access method for retrieving next tuple)
  - [TidRangeRecheck](../T/TidRangeRecheck.md) (recheck method for EvalPlanQual processing)
- Called from (representative examples):
  - [ExecInitTidRangeScan](ExecInitTidRangeScan.md) (during plan node initialization)

## Notes and Other Information
- This function is declared as static, meaning it's only accessible within the nodeTidrangescan.c file
- The function follows the standard PostgreSQL executor pattern by delegating to ExecScan with specialized access methods
- The cursor positioning assumption is critical for correct operation - the AMI must maintain proper state between calls
- The function requires that the relation be pre-opened for TID range scanning before execution begins