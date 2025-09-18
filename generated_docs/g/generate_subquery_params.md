# generate_subquery_params

## Location
src/backend/optimizer/plan/subselect.c: 580 - 612

## Overview
Creates a list of Param nodes representing the output columns of a subquery's target list, used for parameter passing between subqueries and outer queries.

## Definition
```c
static List *generate_subquery_params(PlannerInfo *root, List *tlist, List **paramIds)
```

## Detailed Description
This function processes a subquery's target list and generates corresponding Param nodes that will be used to pass values from the subquery to the outer query. Each non-resjunk entry in the target list results in a new PARAM_EXEC parameter that captures the type information (datatype, typmod, and collation) of the corresponding expression.

The function serves a critical role in subplan execution by establishing the parameter interface between subqueries and their parent queries. It ensures that the outer query can access subquery results through properly typed parameters, maintaining type safety and enabling efficient parameter substitution during execution.

The function skips resjunk entries since these are internal working columns that should not be visible to the outer query.

## Parameters / Member Variables
- `root`: PlannerInfo context for the current query level
- `tlist`: Target list of the subquery whose output columns need parameters
- `paramIds`: Output parameter - pointer to list where parameter IDs will be stored

## Dependencies
- Functions called/Symbols referenced:
  - generate_new_exec_param
  - exprType
  - exprTypmod
  - exprCollation
  - lappend
  - lappend_int
- Called from (representative examples):
  - build_subplan (multiple times for different sublink types)

## Notes and Other Information
- The function is static, meaning it's only accessible within the subselect.c file
- Returns both the list of Param nodes and fills in the paramIds list through the output parameter
- Only processes non-resjunk entries from the target list
- Each generated parameter has the same type information as the corresponding target list expression
- The generated parameters are of type PARAM_EXEC, which are used for inter-plan communication
- Located in src/backend/optimizer/plan/subselect.c:580-612