# is_projection_capable_path

## Location
[src/backend/optimizer/plan/createplan.c:7234-7283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L7234-L7283)

## Overview
Determines whether a given Path node is capable of performing projection (selecting specific columns and computing expressions) as part of its execution.

## Definition
```c
bool
is_projection_capable_path(Path *path)
```

## Detailed Description
The `is_projection_capable_path` function evaluates whether a specific path node type can handle projection operations directly, which helps the planner decide whether to add a separate projection step. Most plan node types can perform projection, so this function uses a negative approach - it explicitly lists the path types that cannot project and returns false for those. The function handles special cases like CustomScan paths (which may support projection based on flags), Append paths (which can project only when representing dummy paths), and ProjectSet paths (which are restricted to prevent target list replacement that could interfere with set-returning functions).

## Parameters / Member Variables
- `path`: Path * - The path node to check for projection capability

## Dependencies
- Functions called/Symbols referenced:
  - castNode (casts path to specific type for CustomPath)
  - CUSTOMPATH_SUPPORT_PROJECTION (flag indicating custom path supports projection)
  - IS_DUMMY_APPEND (macro to check if Append path represents a dummy path)
- Called from (representative examples):
  - create_projection_plan (determines if separate projection node is needed)
  - create_projection_path (path creation logic)
  - apply_projection_to_path (applies projection to paths)

## Notes and Other Information
- Uses a negative filtering approach - assumes most paths can project and lists exceptions
- Non-projection-capable path types include: Hash, Material, Memoize, Sort, IncrementalSort, Unique, SetOp, LockRows, Limit, ModifyTable, MergeAppend, RecursiveUnion
- CustomScan paths can project only if they have the CUSTOMPATH_SUPPORT_PROJECTION flag set  
- Append paths are projection-capable only when they represent dummy paths (IS_DUMMY_APPEND)
- ProjectSet paths are considered non-projection-capable to prevent interference with set-returning function placement
- This function is crucial for the planner's decision on whether to add explicit Projection plan nodes
- The restriction on ProjectSet may be relaxed in future PostgreSQL versions