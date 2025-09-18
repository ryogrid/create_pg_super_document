# mark_async_capable_plan

## Location
src/backend/optimizer/plan/createplan.c: 1141 - 1216

## Overview
Determines and marks whether a Plan node is capable of asynchronous execution based on its corresponding Path node characteristics.

## Definition
```c
static bool mark_async_capable_plan(Plan *plan, Path *path)
```

## Detailed Description
The `mark_async_capable_plan` function evaluates whether a given execution plan node can be executed asynchronously. Asynchronous execution allows PostgreSQL to perform other work while waiting for I/O operations to complete, improving overall query performance. The function examines different path types (SubqueryScan, ForeignPath, ProjectionPath) and applies specific rules to determine async capability. If a plan is deemed async-capable, it sets the async_capable flag on the plan node and returns true. The function is recursive for certain path types like SubqueryScanPath and ProjectionPath.

## Parameters / Member Variables
- `plan`: The Plan node to be evaluated and potentially marked as async-capable
- `path`: The corresponding Path node that was used to create the plan, containing information about the access method

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - trivial_subqueryscan
  - mark_async_capable_plan (recursive call)
  - SubqueryScan (type)
  - FdwRoutine (type)
  - SubqueryScanPath (type)
  - ForeignPath (type)
  - ProjectionPath (type)
  - Result (type)
- Called from (representative examples):
  - create_append_plan
  - mark_async_capable_plan (recursive calls)

## Notes and Other Information
- Plans with gating Result nodes cannot be executed asynchronously due to execution model constraints
- For SubqueryScanPath: Only trivial (deletable) subquery scans with async-capable subplans are considered async-capable
- For ForeignPath: Relies on foreign data wrapper's IsForeignPathAsyncCapable callback to determine capability
- For ProjectionPath: Recursively checks the subpath since create_projection_plan() pulls up the subplan
- The function returns false for all other path types, indicating they cannot be executed asynchronously
- The async_capable flag is set on the plan node when the function determines it can be executed asynchronously
- This functionality is part of PostgreSQL's async append execution feature for parallel query processing