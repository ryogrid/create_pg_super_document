# boolvarsel

## Location
[src/backend/utils/adt/selfuncs.c:1513-1540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1513-L1540)

## Overview
Computes the selectivity of a Boolean variable or expression, providing estimates for query optimization based on statistical data when available.

## Definition

```c
Selectivity
boolvarsel(PlannerInfo *root, Node *arg, int varRelid)
```
## Detailed Description
The  function estimates the selectivity (fraction of rows that would be returned) for a Boolean variable or Boolean-valued expression. It can operate on any boolean-valued expression, but produces the most accurate estimates when the expression involves only variables from the specified relation and when statistics are available for those variables or expressions (particularly if they are indexed).

The function implements the logical equivalence that a Boolean variable V is equivalent to the clause  (V equals true). When statistics are available, it leverages this equivalence to compute selectivity using the  function. If no statistics are available, it falls back to a default estimate of 0.5 (50% selectivity).

This function is a key component in PostgreSQL's cost-based query optimization, helping the planner estimate how many rows will satisfy Boolean conditions and choose optimal execution plans accordingly.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Node representing the Boolean variable or expression to analyze
- : Relation ID to restrict analysis to (0 if no restriction)

## Dependencies
- Functions called/Symbols referenced:
  - examine_variable
  - var_eq_const
  - ReleaseVariableStats
  - VariableStatData
- Called from (representative examples):
  - clause_selectivity_ext
  - GenericCosts

## Notes and Other Information
- Returns a default selectivity of 0.5 when no statistical data is available
- Leverages the equivalence between Boolean variable V and the clause V = 't' for computation
- Part of PostgreSQL's selectivity estimation framework used by the query planner
- Can handle indexed Boolean expressions when statistics are collected on them
- The function properly manages memory by releasing variable statistics data after use