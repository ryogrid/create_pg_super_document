# ProjectSetPath

## Location
[src/include/nodes/pathnodes.h:2185-2189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2185-L2189)

## Overview
ProjectSetPath represents the evaluation of a targetlist containing set-returning functions (SRFs), which requires a ProjectSet plan node to handle the expansion of single input rows into multiple output rows.

## Definition
```c
typedef struct ProjectSetPath
{
    Path        path;
    Path       *subpath;      /* path representing input source */
} ProjectSetPath;
```

## Detailed Description
ProjectSetPath is a specialized path node designed to handle set-returning functions (SRFs) in the SELECT targetlist. Set-returning functions are functions that can return multiple rows from a single input row, such as unnest(), generate_series(), or custom functions that return SETOF types. 

Unlike regular projection operations that maintain a 1:1 relationship between input and output rows, ProjectSetPath handles the 1:N relationship where a single input tuple can generate multiple output tuples. This requires special execution logic implemented by the ProjectSet plan node, which manages the expansion of rows and coordinates multiple SRFs that may return different numbers of rows.

The path planning system recognizes when SRFs are present in the targetlist and creates ProjectSetPath nodes instead of regular ProjectionPath nodes to ensure proper execution semantics.

## Parameters / Member Variables
- `path`: Base Path structure containing cost estimates, output cardinality (reflecting the expanded row count from SRFs), and target information
- `subpath`: Pointer to the input path that provides the source data before SRF expansion

## Dependencies
- Functions called/Symbols referenced:
  - [Path](Path.md) (inherited base structure)
- Called from (representative examples):
  - [create_set_projection_path](../c/create_set_projection_path.md) (path creation)
  - [create_project_set_plan](../c/create_project_set_plan.md) (plan generation)
  - [apply_projection_to_path](../a/apply_projection_to_path.md) (projection application)
  - [is_dummy_rel](../i/is_dummy_rel.md) (optimization checks)

## Notes and Other Information
- [ProjectSetPath](ProjectSetPath.md) is specifically required when the targetlist contains set-returning functions that can expand single input rows into multiple output rows
- Unlike ProjectionPath, there's no \dummy\ optimization available since ProjectSet execution semantics are fundamentally different from regular projection
- The cost model must account for the row multiplication factor when SRFs are present
- Essential for queries using functions like unnest(), generate_series(), json_array_elements(), and user-defined functions returning SETOF types
- Multiple SRFs in the same targetlist are executed in parallel, with the ProjectSet node handling the coordination and NULL-padding when SRFs return different numbers of rows
- The output cardinality estimation is critical for proper query planning as SRFs can dramatically increase the number of output rows