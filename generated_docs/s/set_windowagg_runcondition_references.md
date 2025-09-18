# set_windowagg_runcondition_references

## Location
[src/backend/optimizer/plan/setrefs.c:3412-3438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L3412-L3438)

## Overview
Converts WindowFunc references in window aggregate run conditions to Var references that point to the matching WindowFunc entries in a plan's target list.

## Definition
```c
static List *
set_windowagg_runcondition_references(PlannerInfo *root,
                                      List *runcondition,
                                      Plan *plan)
```

## Detailed Description
This function serves as a higher-level wrapper for fixing references in window aggregate run conditions. It takes a plan node and its associated run conditions, builds an indexed target list from the plan's target list, and then uses that index to convert WindowFunc references in the run conditions to appropriate Var references.

The function follows a standard pattern in PostgreSQL's plan reference fixing: build an index of the target list for efficient lookup, perform the reference transformation, and clean up the index. This ensures that run conditions can properly reference window functions that have been computed and are available in the plan's output.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information  
- `runcondition`: List of condition expressions containing WindowFunc references to be converted
- `plan`: Plan node whose target list contains the WindowFunc entries to reference

## Dependencies
- Functions called/Symbols referenced:
  - [build_tlist_index](../b/build_tlist_index.md)
  - [fix_windowagg_condition_expr](../f/fix_windowagg_condition_expr.md)  
  - [indexed_tlist](../i/indexed_tlist.md) (type)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - fix_scan_list
  - [set_plan_refs](set_plan_refs.md)

## Notes and Other Information
- This is a static function within the setrefs.c module, part of the internal plan reference fixing machinery
- The function creates a temporary indexed target list for efficient lookups and properly cleans it up with pfree()
- Acts as a convenient wrapper that handles the index management around the core fix_windowagg_condition_expr functionality
- Part of the broader plan tree reference fixing process that ensures proper variable resolution after plan construction
- Critical for window aggregate operations where conditions need to reference computed window function results