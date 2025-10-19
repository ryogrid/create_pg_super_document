# TS_execute_ternary

## Location
[src/backend/utils/adt/tsvector_op.c:1871-1882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L1871-L1882)

## Overview
TS_execute_ternary evaluates tsquery boolean expressions and returns the full ternary result, preserving TS_MAYBE values that indicate uncertain matches rather than converting them to boolean true.

## Definition

```c
TSTernaryValue
TS_execute_ternary(QueryItem *curitem, void *arg, uint32 flags,
				   TSExecuteCallback chkcond)
```
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
- `*curitem`: Pointer to the first QueryItem in the tsquery expression tree
- `*arg`: Opaque argument passed through to the TSExecuteCallback function
- `flags`: Execution control flags (bitmask from ts_utils.h)
- `chkcond`: Callback function that checks whether a primitive lexeme value is present
## Dependencies
- Functions called/Symbols referenced:
  - [TS_execute_recurse](TS_execute_recurse.md)
  - TSTernaryValue (return type)
- Called from (representative examples):
  - [gin_tsquery_consistent](../g/gin_tsquery_consistent.md) (in tsginidx.c)
  - [gin_tsquery_triconsistent](../g/gin_tsquery_triconsistent.md) (in tsginidx.c)

## Notes and Other Information
- Critical for GIN index operations where ternary logic enables efficient index scans
- The preservation of TS_MAYBE allows callers to implement recheck logic
- More precise than the boolean TS_execute for scenarios requiring exact match certainty
- Used primarily in index access methods rather than user-facing operations
- The ternary result enables optimizations in index scanning by distinguishing between "definitely matches", "definitely doesn't match", and "needs detailed check"

## Simplified Source

```c
TSTernaryValue TS_execute_ternary(QueryItem *curitem, void *arg, uint32 flags,
                                  TSExecuteCallback chkcond) {
    // Direct passthrough to recursive executor, preserving ternary logic
    return TS_execute_recurse(curitem, arg, flags, chkcond);
}
```