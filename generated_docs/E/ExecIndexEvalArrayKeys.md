# ExecIndexEvalArrayKeys

## Location
src/backend/executor/nodeIndexscan.c: 661 - 739

## Overview
Evaluates array key expressions, decomposes arrays into individual elements, and initializes scankeys for array-based index operations.

## Definition


## Detailed Description
The `ExecIndexEvalArrayKeys` function handles the evaluation and setup of array-based index scan keys. It evaluates array expressions, decomposes them into individual elements, and prepares the scan infrastructure to iterate through array elements during index scans. This is essential for queries using array operators like `ANY` or `ALL` with index scans.

The function evaluates each array expression in the provided context, validates that arrays are non-null and non-empty, and decomposes them into individual elements using PostgreSQL's array deconstruction utilities. It initializes the scan keys with the first element of each array and sets up the iteration state for subsequent calls to `ExecIndexAdvanceArrayKeys`.

If any array is null or empty, the function returns false, indicating that no matches are possible since array operations require at least one element to compare against.

## Parameters
- `econtext`: The expression context containing current execution state and variable values for expression evaluation
- `arrayKeys`: Array of IndexArrayKeyInfo structures containing array expressions and associated scan keys
- `numArrayKeys`: The number of array keys in the arrayKeys array

## Return Value
- `true`: All arrays have been successfully evaluated and contain at least one element; scankeys are initialized with first elements
- `false`: At least one array is null or empty, meaning no matches are possible

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalExpr
  - DatumGetArrayTypeP
  - get_typlenbyvalalign
  - deconstruct_array
  - MemoryContextSwitchTo
- Data types used:
  - IndexArrayKeyInfo
  - ScanKey
  - ExprContext
  - ArrayType
- Constants/Macros used:
  - ARR_ELEMTYPE
  - SK_ISNULL

## Called From
- ExecReScanBitmapIndexScan (src/backend/executor/nodeBitmapIndexscan.c:157)

## Notes and Other Information
- Allocates array element data in per-tuple memory context for proper lifecycle management
- Automatically handles memory cleanup through context reset, avoiding explicit pfree calls
- Sets up iteration state by storing decomposed array elements and initializing next_elem counter
- Handles null elements within arrays by setting SK_ISNULL flag appropriately
- Critical for implementing array comparison operators (ANY, ALL) in index scans
- Must be followed by calls to ExecIndexAdvanceArrayKeys to iterate through remaining array elements
- Early termination on first null/empty array optimizes performance by avoiding unnecessary work