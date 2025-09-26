# TS_execute_ternary

## Location
src/backend/utils/adt/tsvector_op.c: 1871 - 1882

## Overview
TS_execute_ternary evaluates tsquery boolean expressions and returns the full ternary result, preserving TS_MAYBE values that indicate uncertain matches rather than converting them to boolean true.

## Definition


## Detailed Description
This function provides an alternative interface to TS_execute that preserves the full semantic range of tsquery execution results. Unlike TS_execute which converts TS_MAYBE to true, this function returns the complete TSTernaryValue result from the underlying recursive execution.

The function is particularly important for index operations and advanced text search scenarios where the distinction between definite matches (TS_YES), definite non-matches (TS_NO), and uncertain matches (TS_MAYBE) is crucial for correctness and performance.

Key characteristics:
- Direct passthrough to TS_execute_recurse without result conversion
- Preserves TS_MAYBE results for uncertain match scenarios
- Essential for GIN index triconsistent operations
- Enables more sophisticated match logic in advanced use cases

The ternary logic is particularly valuable in index scans where TS_MAYBE indicates that a more detailed check is needed to determine the final result.

## Parameters / Member Variables
- : Pointer to the first QueryItem in the tsquery expression tree
- : Opaque argument passed through to the TSExecuteCallback function  
- : Execution control flags (bitmask from ts_utils.h)
- : Callback function that checks whether a primitive lexeme value is present

## Dependencies
- Functions called/Symbols referenced:
  - TS_execute_recurse
  - TSTernaryValue (return type)
- Called from (representative examples):
  - gin_tsquery_consistent (in tsginidx.c)
  - gin_tsquery_triconsistent (in tsginidx.c)

## Notes and Other Information
- Critical for GIN index operations where ternary logic enables efficient index scans
- The preservation of TS_MAYBE allows callers to implement recheck logic
- More precise than the boolean TS_execute for scenarios requiring exact match certainty
- Used primarily in index access methods rather than user-facing operations
- The ternary result enables optimizations in index scanning by distinguishing between "definitely matches", "definitely doesn't match", and "needs detailed check"