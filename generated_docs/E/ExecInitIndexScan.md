# ExecInitIndexScan

## Location
[src/backend/executor/nodeIndexscan.c:886-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L886-L1134)

## Overview
ExecInitIndexScan initializes the execution state for an index scan node, setting up scan keys, opening relations, and preparing all necessary data structures for index scanning operations.

## Definition

```c
structure
	 */
	indexstate = makeNode(IndexScanState);
```
## Detailed Description
ExecInitIndexScan is the initialization function for index scan execution nodes. It creates and configures an IndexScanState structure containing all the information needed to execute index scans. The function handles both the base relation and index relation setup, as index scans require tracking two separate relations.

The function performs several key operations:
1. Creates and initializes the IndexScanState structure
2. Opens the base relation being scanned
3. Initializes tuple slot and result type information
4. Processes index qualification expressions and ORDER BY expressions
5. Opens the index relation
6. Builds scan keys from index qualifications using ExecIndexBuildScanKeys
7. Sets up ORDER BY processing including sort support if needed
8. Creates runtime expression context for evaluating runtime keys

The function includes special handling for EXPLAIN-only execution, where it stops early to allow index advisor plugins to explain plans with non-existent indexes. It also properly handles runtime keys that need evaluation during scan execution.

## Parameters / Member Variables
- : Pointer to IndexScan plan node containing the index scan specification
- : Execution state containing global execution context and parameters
- : Execution flags controlling initialization behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - ExecAssignExprContext
  - ExecOpenScanRelation
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - [ExecInitExprList](ExecInitExprList.md)
  - exec_rt_fetch
  - [index_open](../i/index_open.md)
  - [ExecIndexBuildScanKeys](ExecIndexBuildScanKeys.md)
  - PrepareSortSupportFromOrderingOp
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - pairingheap_allocate
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (in execProcnode.c:220)

## Notes and Other Information
- The function handles two relations: the base table being scanned and the index being used
- Runtime keys require a separate expression context that is not reset for every tuple
- ORDER BY expressions are processed similarly to index qualifications but require additional sort support setup
- The reorder queue is initialized for handling ORDER BY expression re-checking when needed
- Special EXPLAIN-only mode allows index advisor plugins to work with non-existent indexes
- The function properly manages memory contexts for different types of expression evaluation
- Located in src/backend/executor/nodeIndexscan.c:886-1134