# inline_cte_walker

## Location
[src/backend/optimizer/plan/subselect.c:1151-1253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1151-L1253)

## Overview
A recursive tree walker function that traverses query nodes to locate and replace RTE_CTE references with RTE_SUBQUERY entries, performing the actual CTE inlining by substituting CTE references with their corresponding query definitions.

## Definition


## Detailed Description
This function performs the core work of CTE inlining by recursively traversing the query tree and replacing matching CTE references with inline subqueries. When it encounters a RangeTblEntry that matches the target CTE (by name and nesting level), it transforms the RTE from RTE_CTE type to RTE_SUBQUERY type and substitutes the CTE's query definition.

The function handles three main node types:
1. **Query nodes**: Recursively processes subqueries while tracking nesting levels and using QTW_EXAMINE_RTES_AFTER to ensure range table entries are processed after their contents are examined
2. **RangeTblEntry nodes**: Checks for matching CTE references and performs the actual inlining transformation when found
3. **Other expression nodes**: Delegates processing to standard expression tree walking

Key aspects of the inlining process include:
- **Level adjustment**: Uses  to properly adjust variable references when the CTE is inlined at a different nesting level
- **Type conversion**: Changes the RTE from RTE_CTE to RTE_SUBQUERY and populates the subquery field
- **Field cleanup**: Zeros out CTE-specific fields like ctename, ctelevelsup, and self_reference flags
- **FOR UPDATE handling**: Preserves the historical behavior where FOR UPDATE clauses don't extend into CTEs

## Parameters / Member Variables
- : The current Node being examined in the tree traversal
- : Pointer to inline_cte_walker_context containing the target CTE name, current nesting level, and the CTE query to substitute

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - query_tree_walker
  - expression_tree_walker
  - copyObject
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md)
  - strcmp
  - [QTW_EXAMINE_RTES_AFTER](../Q/QTW_EXAMINE_RTES_AFTER.md) (flag constant)
  - RTE_CTE, RTE_SUBQUERY (enum values)
  - NIL (null list constant)
- Called from (representative examples):
  - [inline_cte](inline_cte.md)
  - [inline_cte_walker](inline_cte_walker.md) (recursive calls)

## Notes and Other Information
- This is a recursive function that calls itself when processing Query nodes and expression nodes
- Uses QTW_EXAMINE_RTES_AFTER to prevent descending into the newly inlined CTE query during the same traversal
- The levelsup counter is incremented when entering subqueries and decremented when exiting to maintain proper scope tracking
- Level adjustment with IncrementVarSublevelsUp ensures that outer references in the inlined query maintain correct scope
- The transformation is destructive, modifying the original query tree in place
- Static function scope limits visibility to the subselect.c compilation unit
- FOR UPDATE semantics are preserved by not extending row-level locking behavior into the inlined subquery
- CTE-specific metadata fields are explicitly cleared after conversion to prevent confusion