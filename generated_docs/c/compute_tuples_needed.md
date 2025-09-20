# compute_tuples_needed

## Location
[src/backend/executor/nodeLimit.c:431-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLimit.c#L431-L446)

## Overview
compute_tuples_needed calculates the maximum number of tuples that a Limit node needs to retrieve from its child to satisfy the LIMIT/OFFSET requirements.

## Definition

```c
static int64
compute_tuples_needed(LimitState *node)
```
## Detailed Description
This function determines the optimal tuple bound that should be communicated to the child plan node for performance optimization. It calculates the total number of tuples needed by adding the OFFSET value to the LIMIT count. However, it returns -1 (indicating unlimited) in cases where the exact requirement cannot be determined in advance.

The function returns -1 in two specific scenarios:
1. When noCount is true (no LIMIT specified, equivalent to LIMIT ALL)
2. When LIMIT_OPTION_WITH_TIES is used, since the actual number of tuples returned depends on how many tuples tie with the last qualifying tuple

For normal LIMIT/OFFSET operations, it returns the sum of count + offset, which represents the maximum position that will be accessed. The function includes overflow protection - if the addition overflows, it returns a negative value which is treated as unlimited.

## Parameters / Member Variables
- : LimitState containing the computed offset, count, noCount flag, and limitOption

## Dependencies
- Functions called/Symbols referenced:
  - None (simple arithmetic and field access)
- Called from (representative examples):
  - [recompute_limits](../r/recompute_limits.md) (to set tuple bound for child node)

## Notes and Other Information
- Used for query optimization by informing child nodes of tuple requirements
- Overflow handling ensures robustness for very large limit values
- The -1 return value follows PostgreSQL conventions for unlimited tuple bounds
- Critical for efficient execution planning, especially with large datasets where early termination can provide significant performance benefits
- The WITH TIES option prevents optimization because the final tuple count is indeterminate until execution