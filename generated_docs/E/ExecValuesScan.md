# ExecValuesScan

## Location
[src/backend/executor/nodeValuesscan.c:196-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeValuesscan.c#L196-L209)

## Overview
ExecValuesScan is the main execution function for VALUES scan nodes, scanning through values lists sequentially and returning the next qualifying tuple.

## Definition
```c
static TupleTableSlot *ExecValuesScan(PlanState *pstate)
```

## Detailed Description
ExecValuesScan serves as the primary entry point for executing VALUES scan operations in PostgreSQL's executor. It follows the standard executor pattern by delegating the actual scanning work to the generic ExecScan() function, providing it with VALUES-specific access method functions.

The function acts as a thin wrapper that casts the generic PlanState to a ValuesScanState and then calls ExecScan with the appropriate callback functions (ValuesNext for tuple retrieval and ValuesRecheck for EvalPlanQual support). This design allows VALUES scans to integrate seamlessly with PostgreSQL's standard scan execution framework.

## Parameters / Member Variables
- `pstate`: Generic PlanState pointer that gets cast to ValuesScanState for VALUES-specific operations

## Dependencies
- Functions called/Symbols referenced:
  - castNode (via ValuesScanState cast)
  - [ExecScan](ExecScan.md)
  - [ValuesNext](../V/ValuesNext.md) (passed as access method)
  - [ValuesRecheck](../V/ValuesRecheck.md) (passed as recheck method)
- Called from:
  - [ExecInitValuesScan](ExecInitValuesScan.md) (indirectly through executor dispatch)

## Notes and Other Information
- Follows PostgreSQL's standard executor pattern by using ExecScan as the core scanning engine
- The function is registered as the execution method when initializing VALUES scan nodes
- Part of the executor's scan node hierarchy, providing a consistent interface for VALUES operations
- The actual tuple generation logic is implemented in ValuesNext, while this function handles the executor integration