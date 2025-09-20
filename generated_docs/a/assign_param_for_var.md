# assign_param_for_var

## Location
[src/backend/optimizer/util/paramassign.c:66-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L66-L119)

## Overview
Selects a PARAM_EXEC number to identify the given Var as a parameter for the current subquery and records the need for the Var in the proper upper-level root->plan_params.

## Definition

```c
static int
assign_param_for_var(PlannerInfo *root, Var *var)
```
## Detailed Description
This function is responsible for parameter assignment during query planning in PostgreSQL's optimizer. It handles the conversion of Var nodes into parameters that can be passed between query levels in nested subqueries. The function first searches for an existing matching PlannerParamItem to avoid creating duplicates, and if none is found, creates a new parameter entry.

The function navigates up the planner hierarchy to find the appropriate query level where the Var belongs, then either reuses an existing parameter or creates a new one. This is crucial for proper handling of correlated subqueries where variables from outer query levels need to be parameterized.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context for the current query level
- : Var node representing a table column reference that needs to be parameterized

## Dependencies
- Functions called/Symbols referenced:
  - PlannerParamItem (structure creation)
  - [bms_equal](../b/bms_equal.md) (bitmap set equality comparison)
  - copyObject (deep copy of the Var node)
  - makeNode (node creation)
  - lappend_oid (append OID to list)
- Called from (representative examples):
  - [replace_outer_var](../r/replace_outer_var.md)

## Notes and Other Information
- The function performs a comparison that matches _equalVar() except for ignoring varlevelsup
- It ignores varnosyn, varattnosyn, and location fields during comparison
- The copied Var has its varlevelsup reset to 0 since it will be used as a parameter
- Parameter IDs are assigned sequentially based on the length of glob->paramExecTypes
- This is a static function within paramassign.c, indicating it's used internally for parameter assignment logic