# RecursiveUnion

## Location
[src/include/nodes/plannodes.h:325-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L325-L346)

## Overview
The RecursiveUnion node generates a recursive union of two subplans, implementing SQL's WITH RECURSIVE functionality for recursive queries.

## Definition
```c
typedef struct RecursiveUnion
{
    Plan        plan;
    int         wtParam;            /* ID of Param representing work table */
    int         numCols;            /* number of columns to check for duplicate-ness */
    AttrNumber *dupColIdx;          /* their indexes in the target list */
    Oid        *dupOperators;       /* equality operators to compare with */
    Oid        *dupCollations;      /* collations for duplicate checking */
    long        numGroups;          /* estimated number of groups in input */
} RecursiveUnion;
```

## Detailed Description
The RecursiveUnion execution node implements PostgreSQL's recursive query functionality, as used in WITH RECURSIVE common table expressions (CTEs). It manages the iterative execution of recursive queries by maintaining a working table and repeatedly executing the recursive term until no new rows are produced.

The node operates with two child plans: the "outer" subplan represents the non-recursive term (the base case), and the "inner" subplan represents the recursive term. The execution starts with the non-recursive term, then iteratively executes the recursive term using results from previous iterations until convergence.

For UNION (not UNION ALL) recursive queries, the node also performs duplicate elimination to ensure each row appears only once in the final result set.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information
- `wtParam`: Parameter ID representing the work table used to pass intermediate results between iterations
- `numCols`: Number of columns to check for duplicate elimination (zero for UNION ALL cases)
- `dupColIdx`: Array of attribute numbers indicating which target list columns to compare for duplicates
- `dupOperators`: Array of OIDs specifying the equality operators used for duplicate comparison
- `dupCollations`: Array of OIDs specifying the collations used for duplicate comparison
- `numGroups`: Estimated number of distinct groups in the input, used for cost estimation

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references from this struct)
- Called from (representative examples):
  - ExecInitRecursiveUnion
  - create_recursiveunion_plan
  - build_hash_table
  - find_recursive_union

## Notes and Other Information
- Essential for implementing SQL WITH RECURSIVE functionality
- The outer subplan is always the non-recursive (base) term
- The inner subplan is always the recursive term
- Maintains a work table to pass results between recursive iterations
- Supports both UNION and UNION ALL semantics
- For UNION ALL, duplicate checking fields are zero/null
- Uses hash tables for efficient duplicate elimination when needed
- Located in src/include/nodes/plannodes.h:325-346