# limit_needed

## Location
[src/backend/optimizer/plan/planner.c:2658-2716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2658-L2716)

## Overview
Determines whether a Limit plan node is actually required by checking if OFFSET and LIMIT clauses have meaningful constant values.

## Definition
```c
bool limit_needed(Query *parse)
```

## Detailed Description
This function performs an optimization check to determine if a Limit plan node needs to be added to the execution plan. The function analyzes the OFFSET and LIMIT clauses to identify cases where a Limit node would be redundant:

1. **LIMIT Analysis**:
   - Returns true if LIMIT has a non-null constant value (actual limit needed)
   - Returns true if LIMIT is a non-constant expression (runtime evaluation needed)
   - Continues checking if LIMIT is NULL (equivalent to LIMIT ALL)

2. **OFFSET Analysis**:
   - Returns true if OFFSET has a non-zero constant value (rows need to be skipped)
   - Returns true if OFFSET is a non-constant expression (runtime evaluation needed)
   - Treats NULL OFFSET as equivalent to OFFSET 0

3. **Optimization Case**:
   - Returns false only when both LIMIT is NULL (or absent) AND OFFSET is 0 (or absent)
   - This handles the common "OFFSET 0" pattern used as an optimization fence

The key distinction from preprocess_limit() is that this function requires hard constants, not estimated values, to make the optimization decision.

## Parameters
- `parse`: Query parse tree containing the LIMIT and OFFSET clauses to examine

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt64](../D/DatumGetInt64.md)
  - IsA, Const node type checking
- Called from:
  - [grouping_planner](../g/grouping_planner.md) (multiple locations)
  - [set_rel_consider_parallel](../s/set_rel_consider_parallel.md)
  - Referenced in GroupPathExtraData structure

## Notes and Other Information
- Provides a performance optimization by avoiding unnecessary Limit nodes at execution time
- The "OFFSET 0" pattern is commonly used as an optimization fence to prevent certain planner optimizations
- Even when this function returns false, OFFSET 0 still functions as an optimization fence because other planner code checks for non-null limitOffset
- Essential for efficient plan generation when LIMIT/OFFSET clauses are purely syntactic
- Located in src/backend/optimizer/plan/planner.c:2658-2716

## Simplified Source

```c
bool limit_needed(Query *parse)
{
    Node *node;

    // Check LIMIT clause
    node = parse->limitCount;
    if (node)
    {
        if (IsA(node, Const))
        {
            // NULL indicates LIMIT ALL (no limit)
            if (!((Const *) node)->constisnull)
                return true;  // LIMIT with actual value
        }
        else
            return true;  // Non-constant LIMIT
    }

    // Check OFFSET clause
    node = parse->limitOffset;
    if (node)
    {
        if (IsA(node, Const))
        {
            // Treat NULL as no offset
            if (!((Const *) node)->constisnull)
            {
                int64 offset = DatumGetInt64(((Const *) node)->constvalue);

                if (offset != 0)
                    return true;  // OFFSET with nonzero value
            }
        }
        else
            return true;  // Non-constant OFFSET
    }

    return false;  // No meaningful limit needed
}
```