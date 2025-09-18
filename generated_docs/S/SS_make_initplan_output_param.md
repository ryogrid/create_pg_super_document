# SS_make_initplan_output_param

## Location
src/backend/optimizer/plan/subselect.c: 3001 - 3016

## Overview
Creates a new Param node to represent the output of an initplan that returns a scalar value of specified type, collation, and typmod.

## Definition
```c
Param *SS_make_initplan_output_param(PlannerInfo *root,
                                     Oid resulttype, int32 resulttypmod,
                                     Oid resultcollation)
```

## Detailed Description
This function creates a parameter node that will hold the output value from an initialization plan (initplan). An initplan is a subquery that needs to be executed once at the beginning of query execution to provide a constant value used elsewhere in the main query. The function allocates a new PARAM_EXEC slot to store the initplan's result and returns a Param node that can be used to reference this value.

The function is a thin wrapper around `generate_new_exec_param` and handles the case where the initplan might not appear in the final plan tree (in which case the allocated PARAM_EXEC slot is simply wasted, which is acceptable).

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and parameter information
- `resulttype`: OID of the data type that the initplan will return
- `resulttypmod`: Type modifier for the result type (e.g., precision for numeric types)
- `resultcollation`: OID of the collation to be used for the result value

## Dependencies
- Functions called/Symbols referenced:
  - [generate_new_exec_param](../g/generate_new_exec_param.md): Creates a new execution parameter with the specified type information
- Called from (representative examples):
  - [preprocess_minmax_aggregates](../p/preprocess_minmax_aggregates.md): Used when converting MIN/MAX aggregates to initplans
  - Referenced in `src/include/optimizer/subselect.h`: Function prototype declaration

## Notes and Other Information
- The function may allocate a PARAM_EXEC slot that goes unused if the initplan is later optimized away, but this is considered acceptable overhead
- This is part of PostgreSQL's subquery processing infrastructure, specifically for handling subqueries that can be converted to initplans for better performance
- The returned Param node will have `paramkind` set to `PARAM_EXEC` and will be assigned a unique `paramid`
- Location: `src/backend/optimizer/plan/subselect.c:3001-3016`