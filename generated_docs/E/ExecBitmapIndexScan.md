# ExecBitmapIndexScan

## Location
[src/backend/executor/nodeBitmapIndexscan.c:38-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapIndexscan.c#L38-L48)

## Overview
ExecBitmapIndexScan is a stub function that serves as a placeholder for pro forma compliance with the executor node interface, but does not support the standard ExecProcNode call convention used by other executor nodes.

## Definition

```c
static TupleTableSlot *
ExecBitmapIndexScan(PlanState *pstate)
```
## Detailed Description
This function is implemented as a deliberate stub that immediately throws an error when called. Unlike other executor node types that support the ExecProcNode call convention for tuple-by-tuple processing, BitmapIndexScan nodes operate differently - they are designed to collect all qualifying tuple identifiers (TIDs) at once during initialization rather than producing tuples one at a time. This design choice reflects the fundamental difference in how bitmap index scans work compared to regular sequential scans.

The function exists purely to maintain consistency with the executor node interface requirements, but its implementation makes it clear that this calling pattern is not supported for bitmap index scan operations.

## Parameters / Member Variables
- : Pointer to the PlanState structure containing the execution state, though this parameter is unused since the function immediately errors out

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
- Called from (representative examples):
  - [ExecInitBitmapIndexScan](ExecInitBitmapIndexScan.md) (sets this as the ExecProcNode function pointer)

## Notes and Other Information
- This is a static function within nodeBitmapIndexscan.c, not exposed to other modules
- The error message clearly indicates that BitmapIndexScan nodes use a different execution model than the standard ExecProcNode convention
- BitmapIndexScan nodes are typically executed through MultiExecBitmapIndexScan instead, which collects all TIDs at once
- Located at src/backend/executor/nodeBitmapIndexscan.c:38-48