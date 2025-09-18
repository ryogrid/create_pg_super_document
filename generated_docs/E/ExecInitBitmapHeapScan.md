# ExecInitBitmapHeapScan

## Location
[src/backend/executor/nodeBitmapHeapscan.c:685-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapHeapscan.c#L685-L783)

## Overview
ExecInitBitmapHeapScan initializes a bitmap heap scan execution node by setting up the scan state, opening relations, initializing child nodes, and configuring tuple slots and projection information for query execution.

## Definition
BitmapHeapScanState *ExecInitBitmapHeapScan(BitmapHeapScan *node, EState *estate, int eflags)

## Detailed Description
ExecInitBitmapHeapScan is responsible for the complete initialization of a bitmap heap scan execution node during query plan startup. This function creates and configures a BitmapHeapScanState structure that will be used throughout the scan's execution lifetime. The initialization process includes several critical steps: creating the execution state structure, opening the target relation, initializing child nodes (typically bitmap index scans), setting up tuple slots and projection information, initializing qualification expressions, and configuring prefetch parameters based on tablespace I/O concurrency settings.

The function also performs important validation checks, ensuring that unsupported execution flags are not specified and that an MVCC-compliant snapshot is being used for the scan. It establishes the connection between the bitmap heap scan node and the generic executor framework by setting the ExecProcNode function pointer to ExecBitmapHeapScan.

## Parameters / Member Variables
- : BitmapHeapScan pointer containing the plan node information including target relation, qualification conditions, and child plan nodes
- : EState pointer containing the execution state and context information for the entire query
- : Integer flags controlling execution behavior, with checks to ensure backward scanning and mark/restore are not requested

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - ExecAssignExprContext
  - ExecOpenScanRelation
  - [ExecInitNode](ExecInitNode.md)
  - outerPlanState (macro)
  - outerPlan (macro)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - RelationGetDescr
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - [get_tablespace_io_concurrency](../g/get_tablespace_io_concurrency.md)
- Validation functions:
  - Assert
  - IsMVCCSnapshot
- Data types referenced:
  - [BitmapHeapScanState](../B/BitmapHeapScanState.md)
  - BitmapHeapScan
  - [EState](EState.md)
  - [Relation](../R/Relation.md)
- Called from:
  - [ExecInitNode](ExecInitNode.md) (src/backend/executor/execProcnode.c:235)
- Referenced in headers:
  - src/include/executor/nodeBitmapHeapscan.h:20

## Notes and Other Information
- This is a public function that serves as the entry point for bitmap heap scan initialization
- The function validates that EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not set, as bitmap heap scans don't support backward scanning or mark/restore functionality
- Requires an MVCC snapshot to ensure proper visibility and consistency during concurrent operations
- Initializes multiple bitmap-related fields to NULL/invalid state, including support for both regular and shared (parallel) bitmap iterators
- Sets up prefetch configuration using tablespace-specific I/O concurrency settings for optimal performance
- The initialization includes both regular qualification expressions and bitmap-specific qualification (bitmapqualorig) for EvalPlanQual processing
- Returns a fully initialized BitmapHeapScanState that's ready for execution
- Part of the standard executor node initialization interface
- Critical for establishing all necessary data structures and relationships before scan execution begins