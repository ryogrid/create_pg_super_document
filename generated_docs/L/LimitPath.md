# LimitPath

## Location
[src/include/nodes/pathnodes.h:2400-2407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2400-L2407)

## Overview
LimitPath represents a query execution path node for applying LIMIT/OFFSET restrictions to limit the number of rows returned by a query, including support for FETCH FIRST WITH TIES semantics.

## Definition
```c
typedef struct LimitPath
{
    Path        path;
    Path       *subpath;        /* path representing input source */
    Node       *limitOffset;    /* OFFSET parameter, or NULL if none */
    Node       *limitCount;     /* COUNT parameter, or NULL if none */
    LimitOption limitOption;    /* FETCH FIRST with ties or exact number */
} LimitPath;
```

## Detailed Description
LimitPath is a path node that implements LIMIT and OFFSET clause functionality in PostgreSQL queries. It wraps a subpath and applies row count restrictions, supporting both traditional LIMIT/OFFSET semantics and the SQL standard FETCH FIRST WITH TIES functionality. The path node preserves the input ordering from its subpath since LIMIT operations require stable sort order to produce deterministic results. Cost calculations are adjusted based on estimated offset and count values to reflect the reduced number of output rows.

## Parameters / Member Variables
- `path`: Base Path structure containing cost estimates, row counts, and execution metadata
- `subpath`: Path node that produces the input data to be limited
- `limitOffset`: Expression tree for the OFFSET value, or NULL if no offset specified
- `limitCount`: Expression tree for the LIMIT/COUNT value, or NULL if no limit specified
- `limitOption`: Enum value specifying limit behavior (exact count vs. WITH TIES semantics)

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - [Node](../N/Node.md) (for limit expressions)
  - [LimitOption](LimitOption.md) (limit behavior enum)
- Called from (representative examples):
  - [create_limit_path](../c/create_limit_path.md) (pathnode.c:3832)
  - [create_limit_plan](../c/create_limit_plan.md) (createplan.c:2856)
  - [create_plan_recurse](../c/create_plan_recurse.md) (createplan.c:538)

## Notes and Other Information
- Preserves pathkeys from the subpath since LIMIT requires stable ordering for deterministic results
- Cost adjustments account for reduced output rows using adjust_limit_rows_costs() function
- Can be parallel-safe if the subpath is parallel-safe and the relation allows parallel processing
- WITH TIES option requires additional processing to extract sort column information for row comparison
- Does not project data, so it passes through the subpath's target list unchanged
- OFFSET and LIMIT expressions are stored as Node trees and evaluated at execution time
- Cost estimation uses preprocess_limit() estimates where 0 means clause absent and -1 means present but value unknown