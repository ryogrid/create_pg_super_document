# ExecScan

## Location
[src/backend/executor/execScan.c:156-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execScan.c#L156-L269)

## Overview
ExecScan is the core tuple processing function in PostgreSQL's scan execution framework that coordinates tuple retrieval, qualification checking, and projection operations for all scan node types.

## Definition


## Detailed Description
ExecScan serves as the central orchestrator for scan operations in PostgreSQL's executor. It implements a standardized tuple processing loop that works across all scan types (sequential, index, bitmap, foreign, etc.). The function repeatedly fetches tuples using the provided access method, evaluates qualification conditions, and applies projections as needed.

The function employs several optimizations: it bypasses qualification and projection overhead when neither is needed, efficiently handles memory context resets, and provides early termination when no more tuples are available. It maintains the tuple processing pipeline by coordinating between tuple fetching (via ExecScanFetch), qualification evaluation (via ExecQual), and result projection (via ExecProject).

## Parameters / Member Variables
- : The ScanState containing execution state, qualification expressions, and projection information
- : Function pointer to access method-specific tuple retrieval routine (e.g., heap_getnext, index_getnext)
- : Function pointer to recheck access method-specific conditions during EvalPlanQual scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [ExecScanFetch](ExecScanFetch.md) (tuple fetching with EPQ handling)
  - ResetExprContext (memory context management)
  - TupIsNull (tuple validity checking)
  - ExecQual (qualification evaluation)
  - ExecProject (result projection)
  - ExecClearTuple (tuple slot clearing)
  - InstrCountFiltered1 (instrumentation for filtered tuples)
- Data structures used:
  - [ScanState](../S/ScanState.md)
  - [ProjectionInfo](../P/ProjectionInfo.md)
  - ExprContext
  - ExprState
  - TupleTableSlot
- Called from (representative examples):
  - [ExecSeqScan](ExecSeqScan.md) (sequential scan execution)
  - [ExecIndexScan](ExecIndexScan.md) (index scan execution)
  - [ExecBitmapHeapScan](ExecBitmapHeapScan.md) (bitmap heap scan execution)
  - [ExecForeignScan](ExecForeignScan.md) (foreign scan execution)
  - All other scan node execution functions

## Notes and Other Information
- This function implements the standard scan execution pattern used by all scan node types in PostgreSQL
- Performance optimization: directly returns raw scan tuple when no qualification or projection is needed
- Proper memory management through strategic ResetExprContext calls prevents memory leaks during long scans
- The infinite loop with qualification checking ensures only qualifying tuples are returned
- Instrumentation support tracks filtered tuple counts for query performance analysis
- The function maintains cursor semantics as required by the access method interface
- Projection result slot is used for consistent tuple descriptor handling when returning empty results