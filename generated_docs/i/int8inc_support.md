# int8inc_support

## Location
[src/backend/utils/adt/int8.c:826-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L826-L865)

## Overview
A prosupport function for int8inc() and int8inc_any() that provides window function monotonicity analysis for query optimization.

## Definition
Datum int8inc_support(PG_FUNCTION_ARGS)

## Detailed Description
int8inc_support is a support function that analyzes the monotonic properties of int8inc operations within window functions. It examines window frame options and ordering clauses to determine whether the increment function behaves monotonically (always increasing, decreasing, or both) within a given window context. This information is used by the PostgreSQL query planner for optimization purposes, particularly for window function processing.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Contains a Node pointer to a SupportRequestWFuncMonotonic request structure

## Dependencies
- Functions called/Symbols referenced:
  - [SupportRequestWFuncMonotonic](../S/SupportRequestWFuncMonotonic.md)
  - [MonotonicFunction](../M/MonotonicFunction.md)
  - MONOTONICFUNC_NONE
  - MONOTONICFUNC_BOTH
  - MONOTONICFUNC_INCREASING
  - MONOTONICFUNC_DECREASING
  - FRAMEOPTION_START_UNBOUNDED_PRECEDING
  - FRAMEOPTION_END_UNBOUNDED_FOLLOWING
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
This function specifically handles SupportRequestWFuncMonotonic requests to determine monotonicity characteristics based on window frame options. When no ORDER BY clause is present, all rows are considered peers and the function is both monotonically increasing and decreasing. With frame bounds at window start or end, the function determines appropriate monotonic behavior for optimization. The function is defined in src/backend/utils/adt/int8.c:826-865.

## Simplified Source

```c
Datum
int8inc_support(PG_FUNCTION_ARGS)
{
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);

    // Handle window function monotonicity analysis requests
    if (IsA(rawreq, SupportRequestWFuncMonotonic)) {
        SupportRequestWFuncMonotonic *req = (SupportRequestWFuncMonotonic *) rawreq;
        MonotonicFunction monotonic = MONOTONICFUNC_NONE;
        int frameOptions = req->window_clause->frameOptions;

        // No ORDER BY: all rows are peers, function is both increasing and decreasing
        if (req->window_clause->orderClause == NIL) {
            monotonic = MONOTONICFUNC_BOTH;
        } else {
            // Check frame bounds to determine monotonic behavior
            if (frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)
                monotonic |= MONOTONICFUNC_INCREASING;
            if (frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING)
                monotonic |= MONOTONICFUNC_DECREASING;
        }

        req->monotonic = monotonic;
        PG_RETURN_POINTER(req);
    }

    PG_RETURN_POINTER(NULL);
}
```