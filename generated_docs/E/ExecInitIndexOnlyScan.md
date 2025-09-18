# ExecInitIndexOnlyScan

## Location
src/backend/executor/nodeIndexonlyscan.c: 506 - 705

## Overview
ExecInitIndexOnlyScan initializes the execution state for an index-only scan node, setting up scan keys, opening relations, and configuring all necessary data structures for index-only scan operations.

## Definition
```c
IndexOnlyScanState *ExecInitIndexOnlyScan(IndexOnlyScan *node, EState *estate, int eflags)
```

## Detailed Description
This function performs comprehensive initialization for index-only scan operations, which are a critical optimization in PostgreSQL that allows retrieving data directly from index pages without accessing the heap table when all required columns are available in the index.

The initialization process involves multiple phases:

1. **State Structure Creation**: Creates and initializes the IndexOnlyScanState node with proper executor framework integration
2. **Expression Context Setup**: Establishes expression evaluation contexts for runtime computations
3. **Relation Management**: Opens both the base relation and index relation with appropriate lock modes
4. **Tuple Descriptor Setup**: Creates tuple descriptors based on the index target list rather than physical index structure
5. **Slot Allocation**: Allocates tuple slots for both index tuples and table tuples (needed for visibility rechecking)
6. **Projection Setup**: Configures result type and projection information with INDEX_VAR variable references
7. **Qualification Setup**: Initializes both scan qualifications and recheck qualifications
8. **Scan Key Construction**: Builds scan keys from index qualifications and ORDER BY expressions
9. **Runtime Key Handling**: Sets up separate expression context for runtime key evaluation
10. **Name Type Optimization**: Detects and handles the special case where btree indexes store cstrings for name types

The function includes sophisticated handling of the "name" data type optimization where btree indexes store cstrings instead of full name values for efficiency, requiring special conversion logic during tuple storage.

## Parameters / Member Variables
- `node`: Pointer to the IndexOnlyScan plan node containing scan specifications and target information
- `estate`: Execution state containing transaction context, tuple tables, and other execution resources
- `eflags`: Execution flags that control initialization behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY for plan explanation)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - ExecAssignExprContext
  - ExecOpenScanRelation
  - [ExecTypeFromTL](ExecTypeFromTL.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecAllocTableSlot](ExecAllocTableSlot.md)
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfoWithVarno](ExecAssignScanProjectionInfoWithVarno.md)
  - [ExecInitQual](ExecInitQual.md)
  - exec_rt_fetch
  - [index_open](../i/index_open.md)
  - [ExecIndexBuildScanKeys](ExecIndexBuildScanKeys.md)
- Types used:
  - IndexOnlyScan
  - [IndexOnlyScanState](../I/IndexOnlyScanState.md)
  - [EState](EState.md)
  - [TupleDesc](../T/TupleDesc.md)
  - AttrNumber
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- The function distinguishes between regular scans and EXPLAIN-only operations, stopping early for the latter
- Index-only scans maintain two tuple slots: one for index data and one for table data (used during visibility rechecking)
- Runtime keys require a separate expression context to avoid conflicts with per-tuple context resets
- The name type optimization is a btree-specific performance enhancement that may be adopted by other index access methods
- Proper lock mode management ensures consistency with the overall transaction isolation level
- The function handles both index qualification and ORDER BY expressions as scan keys
- Memory allocation uses palloc for PostgreSQL's memory context management