# ExecSort

## Location
src/backend/executor/nodeSort.c: 50 - 220

## Overview
Executes tuple sorting operations, either sorting all tuples from the outer subtree using tuplesort and returning them one by one, with optimization for single-column datum sorts versus multi-column tuple sorts.

## Definition


## Detailed Description
ExecSort is the main execution function for Sort plan nodes in PostgreSQL's executor. It operates in two phases:

1. **Initial sorting phase** (when  is false): Reads all tuples from the outer subtree, feeds them to the tuplesort module, and performs the complete sort operation. The function optimizes performance by choosing between two sorting strategies:
   - **Datum sort**: When sorting a single column, which is significantly faster for pass-by-value types
   - **Tuple sort**: When sorting multiple columns or complex data types

2. **Tuple retrieval phase**: On subsequent calls, returns pre-sorted tuples one by one from the tuplesort state.

The function supports various tuplesort options including random access, bounded sorts, and parallel worker statistics collection. It handles both forward and backward scan directions and manages memory efficiently through the work_mem parameter.

## Parameters / Member Variables
- : The PlanState structure containing the SortState node and execution context

## Dependencies
- Functions called/Symbols referenced:
  - : Cast pstate to SortState
  - : Check for query cancellation
  - : Get the outer plan state
  - : Get result tuple descriptor from outer node
  - : Initialize datum-based tuplesort
  - : Initialize tuple-based tuplesort  
  - : Set bound for bounded sorts
  - : Execute outer plan node to get tuples
  - /: Feed data to tuplesort
  - : Complete the sorting operation
  - /: Retrieve sorted tuples
  - /: Manage result tuple slots
- Called from (representative examples):
  - : During sort node initialization

## Notes and Other Information
- The function uses conditional compilation with SO1_printf for debugging output
- Supports parallel execution with worker statistics collection via shared_info
- Optimizes memory usage by choosing appropriate sorting strategy based on data characteristics  
- Handles scan direction changes by temporarily forcing ForwardScanDirection during initial sort phase
- Manages bounded sorts through tuplesort_set_bound for memory efficiency in TOP-N queries