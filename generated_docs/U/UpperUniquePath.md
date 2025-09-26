# UpperUniquePath

## Location
[src/include/nodes/pathnodes.h:2239-2244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2239-L2244)

## Overview
UpperUniquePath represents a query execution path node that performs adjacent-duplicate removal on presorted input data, implementing DISTINCT operations efficiently on already sorted data.

## Definition
```c
typedef struct UpperUniquePath
{
    Path        path;
    Path       *subpath;        /* path representing input source */
    int         numkeys;        /* number of pathkey columns to compare */
} UpperUniquePath;
```

## Detailed Description
UpperUniquePath is a specialized path node in PostgreSQL's query planner that represents duplicate elimination operations performed on already sorted input data. It assumes the input is presorted according to the columns that need to be made unique, allowing for efficient streaming duplicate removal by comparing only adjacent rows.

This path type is particularly useful for implementing DISTINCT operations and similar duplicate removal scenarios where the input is already appropriately ordered. The algorithm compares the first numkeys columns of consecutive rows and removes duplicates, making it much more efficient than hash-based approaches for sorted data.

The path inherits the base Path structure and adds specific information needed for the unique operation, including the number of key columns to compare for uniqueness determination.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information (cost, parent relation, target, pathkeys, etc.)
- `subpath`: Pointer to the input Path node that provides the presorted source data
- `numkeys`: Number of leading pathkey columns to compare when determining uniqueness

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - pathkeys (from subpath for ordering information)
- Called from (representative examples):
  - [create_upper_unique_path](../c/create_upper_unique_path.md) (creates UpperUniquePath instances)
  - [create_upper_unique_plan](../c/create_upper_unique_plan.md) (converts UpperUniquePath to execution plan)
  - [create_plan_recurse](../c/create_plan_recurse.md) (part of plan creation process)

## Notes and Other Information
- The input data must be presorted on the columns specified by the first numkeys pathkeys
- This path type preserves the input ordering, making it suitable for chaining with other operations
- Cost estimation accounts for CPU comparison costs per tuple and column
- The estimated output row count is typically much lower than input due to duplicate removal
- More efficient than hash-based unique operations when input is already sorted
- Commonly used in upper planning phases for implementing DISTINCT clauses