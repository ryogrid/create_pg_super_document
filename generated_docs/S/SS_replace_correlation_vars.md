# SS_replace_correlation_vars

## Location
[src/backend/optimizer/plan/subselect.c:1868-1874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1868-L1874)

## Overview
Replaces correlation variables (uplevel variables) with Params in expressions, enabling proper parameter passing between parent and child query levels during subquery execution.

## Definition

```c
Node *
SS_replace_correlation_vars(PlannerInfo *root, Node *expr)
```
## Detailed Description
This function serves as the main entry point for replacing uplevel variables with execution parameters. It handles the critical task of converting variables that reference parent query levels into Params that can be passed as arguments to subplans during execution.

The function is part of PostgreSQL's subquery correlation handling system and must run immediately after  to ensure proper timing in the query planning pipeline. It not only replaces simple correlation variables but also handles:
- Uplevel PlaceHolderVars (PHVs)
- Uplevel aggregate functions
- GROUPING() expressions  
- MergeSupportFuncs

The timing is critical because it operates on expressions after sublinks have been converted to subplans, but before the expressions are finalized. This ensures that nested levels of correlation variables are properly handled through recursive application across query levels.

A key aspect of this function is its selective approach: it doesn't recurse into arguments of uplevel PHVs and aggregates, allowing those arguments to be processed at the appropriate parent level when  is called there.

## Parameters / Member Variables
- : PlannerInfo structure containing the current query planning context
- : The expression tree in which to replace correlation variables with Params

## Dependencies
- Functions called/Symbols referenced:
  - : The actual tree-walking function that performs the replacements
- Called from (representative examples):
  - : Main expression preprocessing entry point that calls this for correlation variable handling

## Notes and Other Information
- Returns the modified expression tree with correlation variables replaced by Params
- Must be called immediately after  for correct operation
- The function doesn't perform setup - it directly delegates to the mutator function for tree walking
- Critical for proper subplan parameter passing and execution
- Handles complex nested scenarios where PHV/aggregate arguments contain further-up variables
- SubLinks in uplevel PHV/aggregate arguments are handled by recursive  calls
- Part of the broader subquery correlation variable resolution system in PostgreSQL's planner

## Simplified Source

```c
Node *SS_replace_correlation_vars(PlannerInfo *root, Node *expr)
{
    // Direct delegation to the tree-walking mutator function
    return replace_correlation_vars_mutator(expr, root);
}
```