# pull_up_simple_values

## Location
[src/backend/optimizer/prep/prepjointree.c:1799-1894](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L1799-L1894)

## Overview
Optimizes simple VALUES clauses by pulling them up and replacing the VALUES RTE with a RESULT RTE, eliminating unnecessary subquery structure for single-row value expressions.

## Definition


## Detailed Description
This function performs a specialized optimization for simple VALUES clauses by completely eliminating the VALUES RTE (Range Table Entry) and replacing it with a more efficient RESULT RTE. The optimization applies to VALUES clauses that contain only a single row of constant values.

The process involves several key steps:

1. **Values List Processing**: Creates a modifiable copy of the single VALUES row and converts it into a target list format compatible with the pullup machinery.

2. **Context Setup**: Initializes a  structure that treats the VALUES expressions as if they were a subquery target list, enabling the use of the standard variable replacement infrastructure.

3. **Variable Replacement**: Uses  to replace all references to the VALUES RTE's outputs throughout the query tree with the actual VALUES expressions.

4. **RTE Replacement**: Completely replaces the VALUES RTE with a RESULT RTE, which is more efficient for constant expressions since it doesn't require table scanning infrastructure.

This optimization is particularly valuable because it eliminates the overhead of treating simple constant values as a table scan operation, instead allowing them to be evaluated directly as expressions.

## Parameters / Member Variables
- : PlannerInfo containing the overall query planning context and structures
- : RangeTblRef node that references the VALUES RTE to be pulled up (returned as the replacement node)
- : The RangeTblEntry of type RTE_VALUES that contains the single-row VALUES clause to be optimized

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - linitial
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - lappend
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - lfirst
  - [palloc0](palloc0.md)
  - [perform_pullup_replace_vars](perform_pullup_replace_vars.md)
  - makeNode
  - [makeAlias](../m/makeAlias.md)
  - list_make1
- Called from (representative examples):
  - [pull_up_subqueries_recurse](pull_up_subqueries_recurse.md)

## Notes and Other Information
- The function is static, limiting its scope to the prepjointree.c compilation unit
- Only handles VALUES clauses with exactly one row (single values list), as verified by assertions
- The function assumes no level-zero variables exist in the VALUES list, which is verified by assertion
- Creates a dummy target list structure to leverage existing pullup infrastructure designed for subqueries
- The replacement RESULT RTE uses a generic alias "*RESULT*" since it doesn't correspond to any actual table
- The optimization assumes no outer joins, appendrels, or PlaceHolderVars exist in the context, which is appropriate for simple VALUES usage
- Returns the original jtnode since the RTE replacement occurs in-place in the range table
- This is part of PostgreSQL's broader strategy to eliminate unnecessary query structure layers where semantically safe