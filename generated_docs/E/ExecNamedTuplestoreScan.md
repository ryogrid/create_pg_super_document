# ExecNamedTuplestoreScan

## Location
[src/backend/executor/nodeNamedtuplestorescan.c:67-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeNamedtuplestorescan.c#L67-L81)

## Overview
The main execution function for named tuple store scans that sequentially scans CTE (Common Table Expression) data and returns qualifying tuples using the standard ExecScan framework.

## Definition
```c
static TupleTableSlot *
ExecNamedTuplestoreScan(PlanState *pstate)
```

## Detailed Description
ExecNamedTuplestoreScan serves as the primary execution function for NamedTuplestoreScan plan nodes. It implements the scanning of Common Table Expression (CTE) data that has been stored in a named tuple store. The function acts as a wrapper around the generic ExecScan framework, providing it with the specific access method functions needed for named tuple store operations.

The function casts the generic PlanState to a NamedTuplestoreScanState and delegates the actual scanning logic to ExecScan, passing specialized function pointers for tuple retrieval (NamedTuplestoreScanNext) and tuple rechecking (NamedTuplestoreScanRecheck). This design follows PostgreSQL's executor pattern of using generic scan frameworks with node-specific access methods.

## Parameters / Member Variables
- `pstate`: Generic PlanState pointer that gets cast to NamedTuplestoreScanState, containing the plan node state and execution context

## Dependencies
- Functions called/Symbols referenced:
  - castNode: Safely casts PlanState to NamedTuplestoreScanState with type checking
  - [ExecScan](ExecScan.md): Generic scan framework that handles the scanning logic using provided access methods
  - [NamedTuplestoreScanNext](../N/NamedTuplestoreScanNext.md): Access method function for retrieving the next tuple
  - [NamedTuplestoreScanRecheck](../N/NamedTuplestoreScanRecheck.md): Access method function for tuple rechecking in EvalPlanQual
- Called from (representative examples):
  - [ExecInitNamedTuplestoreScan](ExecInitNamedTuplestoreScan.md): Sets this function as the execution method during node initialization

## Notes and Other Information
- This is a static function, only accessible within nodeNamedtuplestorescan.c
- Follows the standard PostgreSQL executor pattern of using ExecScan with node-specific access methods
- Designed specifically for scanning CTE data stored in named tuple stores
- The function pointer is typically set during node initialization as the main execution method
- Returns TupleTableSlot containing the next qualifying tuple or NULL when scan is complete
- Integrates with PostgreSQL's standard execution framework and supports EvalPlanQual operations