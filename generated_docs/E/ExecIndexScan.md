# ExecIndexScan

## Location
src/backend/executor/nodeIndexscan.c: 519 - 550

## Overview
The main execution function for PostgreSQL's index scan node, responsible for retrieving tuples from an index while handling runtime keys and choosing appropriate scan methods based on ordering requirements.

## Definition
```c
static TupleTableSlot *ExecIndexScan(PlanState *pstate)
```

## Detailed Description
ExecIndexScan serves as the primary entry point for executing index scan operations in PostgreSQL's executor. The function first handles runtime key setup if necessary, then delegates to the appropriate scanning method based on whether ORDER BY keys are present. For scans with ordering requirements (typically KNN searches), it uses IndexNextWithReorder which employs the reorder queue mechanism. For standard scans without ordering, it uses the simpler IndexNext method.

The function intelligently chooses between two execution paths: one optimized for ordered results using a reorder queue, and another for standard index traversal. This design allows PostgreSQL to efficiently handle both regular index scans and more complex operations like K-nearest neighbor searches.

## Parameters / Member Variables
- `pstate`: PlanState pointer that is cast to IndexScanState, containing the scan configuration and runtime state

## Return Value
- Returns a TupleTableSlot containing the next tuple from the index scan, or NULL when no more tuples are available

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - [ExecReScan](ExecReScan.md)
  - [ExecScan](ExecScan.md)
  - [IndexNextWithReorder](../I/IndexNextWithReorder.md)
  - [IndexNext](../I/IndexNext.md)
  - [IndexRecheck](../I/IndexRecheck.md)
- Called from (representative examples):
  - [ExecInitIndexScan](ExecInitIndexScan.md) (assigned as the scan method during initialization)

## Notes and Other Information
- Part of PostgreSQL's executor node framework for plan execution
- Automatically handles runtime key setup through ExecReScan when needed
- The function is static, indicating it's only used within the nodeIndexscan.c file
- Integrates with the broader executor framework through the ExecScan infrastructure
- Supports both standard index scans and ordered scans with reorder queues for KNN operations
- Runtime keys allow for dynamic query parameter binding during execution