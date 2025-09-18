# ExecSeqScan

## Location
[src/backend/executor/nodeSeqscan.c:108-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSeqscan.c#L108-L122)

## Overview
ExecSeqScan is the main execution function for sequential scans that retrieves the next qualifying tuple by delegating to the generic ExecScan routine with sequential scan-specific access methods.

## Definition
```c
static TupleTableSlot *ExecSeqScan(PlanState *pstate)
```

## Detailed Description
ExecSeqScan serves as the primary execution interface for sequential scan operations in PostgreSQL's executor. Rather than implementing scan logic directly, it follows the template method pattern by calling the generic ExecScan function and passing sequential scan-specific access method functions (SeqNext for tuple retrieval and SeqRecheck for EvalPlanQual processing). This design promotes code reuse across different scan types while allowing each scan method to provide its own specialized tuple access and validation logic.

## Parameters / Member Variables
- `pstate`: PlanState pointer that is cast to SeqScanState, containing the execution state for the sequential scan node

## Dependencies
- Functions called/Symbols referenced:
  - [ExecScan](ExecScan.md)
  - [SeqNext](../S/SeqNext.md)
  - [SeqRecheck](../S/SeqRecheck.md)
  - [SeqScanState](../S/SeqScanState.md) (via castNode)
- Called from (representative examples):
  - [ExecInitSeqScan](ExecInitSeqScan.md)

## Notes and Other Information
- This is a static function, only accessible within nodeSeqscan.c
- Uses the template method pattern by delegating to ExecScan with scan-specific methods
- The castNode macro is used for safe type conversion from PlanState to SeqScanState
- Part of PostgreSQL's modular executor architecture where each scan type provides its own execution function
- Returns NULL when no more qualifying tuples are found