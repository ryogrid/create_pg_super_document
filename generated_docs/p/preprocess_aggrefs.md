# preprocess_aggrefs

## Location
src/backend/optimizer/prep/prepagg.c: 110 - 115

## Overview
Resolves the transition type of all Aggrefs in a query clause and determines which aggregates can share aggregate or transition state for optimization purposes.

## Definition
```c
void preprocess_aggrefs(PlannerInfo *root, Node *clause)
```

## Detailed Description
This function serves as the main entry point for preprocessing aggregate functions (Aggrefs) in PostgreSQL's query planner. It performs critical optimizations by:

1. **Resolving transition types**: Determines the appropriate transition data type for each aggregate function
2. **Detecting duplicate aggregates**: Identifies identical aggregate function calls that can share the same state and final values
3. **Finding compatible aggregates**: Locates different aggregate functions that can share the same transition state (e.g., AVG and STDDEV on the same column)

The function modifies Aggrefs in-place, filling in the 'aggtranstype', 'aggno', and 'aggtransno' fields. Information about aggregates and transition functions is collected in the root->agginfos and root->aggtransinfos lists.

Key optimization scenarios handled:
- **Identical aggregates**: Same function calls (e.g., multiple SUM(x)) share the same 'aggno' value
- **Compatible aggregates**: Different functions with same arguments, transition functions, and initial values (e.g., AVG(x) and STDDEV(x)) share transition state but have separate final functions

For optimizations to be valid, all aggregate properties used in the transition phase must be identical, including ORDER BY, DISTINCT, FILTER modifiers, and arguments must not contain volatile functions.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Node representing the query clause containing aggregate functions to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [preprocess_aggrefs_walker](preprocess_aggrefs_walker.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md) (in src/backend/optimizer/plan/planner.c)

## Notes and Other Information
- This function modifies the input expression tree in-place
- Critical for aggregate optimization in PostgreSQL's cost-based optimizer
- Must be called during the planning phase before aggregate nodes are created
- The actual recursive processing is delegated to preprocess_aggrefs_walker
- Final functions must be nondestructive of transition state for optimizations to work