# replace_outer_grouping

## Location
[src/backend/optimizer/util/paramassign.c:270-316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L270-L316)

## Overview
Generates a Param node to replace a GroupingFunc expression that references an outer query level, facilitating parameter passing between query levels in nested subqueries.

## Definition

```c
Param *
replace_outer_grouping(PlannerInfo *root, GroupingFunc *grp)
```
## Detailed Description
This function is part of PostgreSQL's parameter assignment mechanism for handling GroupingFunc expressions that reference outer query levels (agglevelsup > 0). When the optimizer encounters a GroupingFunc that needs to be evaluated at a higher query level, this function creates a parameter placeholder (Param node) to represent it in the current query level.

The function performs several key operations:
1. Navigates up the query level hierarchy to find the appropriate root where the GroupingFunc should be evaluated
2. Creates a copy of the GroupingFunc and adjusts its level references to make it local to the target query level
3. Creates a new PlannerParamItem to track the parameter requirement
4. Registers the parameter type in the global parameter execution types list
5. Returns a Param node that serves as a placeholder for the GroupingFunc value

The approach deliberately avoids de-duplication of outer aggregate references, creating a new parameter slot for each reference to maintain simplicity and correctness.

## Parameters / Member Variables
- : PlannerInfo pointer representing the current query level's planning context
- : GroupingFunc pointer to the grouping function expression that references an outer query level (must have agglevelsup > 0)

## Dependencies
- Functions called/Symbols referenced:
  - copyObject: Creates a deep copy of the GroupingFunc
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md): Adjusts variable level references in the copied GroupingFunc
  - makeNode: Creates new PlannerParamItem and Param nodes
  - lappend_oid: Appends parameter type to the global parameter types list
  - lappend: Adds the parameter item to the plan parameters list
  - exprType: Determines the data type of the GroupingFunc expression

- Called from (representative examples):
  - [replace_correlation_vars_mutator](replace_correlation_vars_mutator.md): Used during correlation variable replacement in subquery planning

## Notes and Other Information
- The function asserts that agglevelsup > 0 and agglevelsup < root->query_level to ensure valid outer reference
- Each call creates a new parameter slot rather than attempting de-duplication for simplicity
- The resulting Param node uses PARAM_EXEC parameter kind, indicating it's an execution-time parameter
- Parameter type modifier is set to -1 (no specific modifier) and collation ID is InvalidOid
- The location information from the original GroupingFunc is preserved in the Param node