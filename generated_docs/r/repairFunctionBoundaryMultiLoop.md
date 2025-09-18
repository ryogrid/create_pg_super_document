# repairFunctionBoundaryMultiLoop

## Location
src/bin/pg_dump/pg_dump_sort.c: 1035 - 1055

## Overview
Repairs circular dependencies involving functions in pg_dump by removing pre-data phase constraints and postponing function definitions to the post-data phase when function splitting is not feasible.

## Definition
```c
static void repairFunctionBoundaryMultiLoop(DumpableObject *boundaryobj, DumpableObject *nextobj)
```

## Detailed Description
This function handles multi-object dependency loops that involve functions, which cannot be resolved by splitting the function into multiple DumpableObjects. Similar to the materialized view repair function, it uses a stopgap approach:

1. Removes the boundary object's dependency on the next object in the loop
2. If the next object is a function (objType == DO_FUNC), marks it as postponed

The postponement mechanism moves the function definition from the pre-data phase to the post-data phase. This approach is particularly useful for handling cases where functions depend on unique indexes, which can occur when functions contain GROUP BY clauses or similar constructs.

This is acknowledged as a workaround solution since the ideal approach of splitting functions into multiple DumpableObjects is not currently implemented.

## Parameters / Member Variables
- `boundaryobj`: Pointer to the DumpableObject that represents the boundary/source of the dependency in the loop
- `nextobj`: Pointer to the DumpableObject that follows the boundary object in the dependency cycle (potentially a function or related object)

## Dependencies
- Functions called/Symbols referenced:
  - [removeObjectDependency](removeObjectDependency.md) (removes the problematic dependency relationship)
  - DO_FUNC (constant identifying function-type objects)
  - DumpableObject (base structure for dumpable database objects)
  - FuncInfo (structure containing function-specific information)
- Called from:
  - [repairDependencyLoop](repairDependencyLoop.md) (main dependency loop repair dispatcher)

## Notes and Other Information
- This is explicitly described as a stopgap solution since proper function splitting is not currently implemented
- The function addresses cases where functions depend on unique indexes, commonly seen with GROUP BY operations in function definitions
- The postponed_def flag causes the CREATE FUNCTION statement to be deferred to the post-data phase
- This approach parallels the strategy used for materialized views in repairMatViewBoundaryMultiLoop
- The solution prioritizes successful dump completion over optimal dump ordering when complex dependency cycles occur involving functions