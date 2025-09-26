# generate_subquery_vars

## Location
[src/backend/optimizer/plan/subselect.c:613-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L613-L641)

## Overview
Builds a list of Var nodes representing the output columns of a subquery's target list, using a specified varno (RTE index) for subquery-to-join conversions.

## Definition
```c
static List *generate_subquery_vars(PlannerInfo *root, List *tlist, Index varno)
```

## Detailed Description
This function creates Var nodes that represent the output columns of a subquery, which is primarily used when converting sublinks to joins. Each non-resjunk entry in the target list is converted to a Var node with the specified varno (relation table entry index), allowing the subquery's output to be referenced as if it were columns from a regular table or view.

This transformation is essential for sublink-to-join conversion optimizations, where correlated subqueries are rewritten as joins for better performance. The generated Var nodes maintain the type information and column references needed for the join operation while providing the proper varno that identifies the subquery in the range table.

The function skips resjunk entries since these are internal columns that shouldn't be visible in the join context.

## Parameters / Member Variables
- `root`: PlannerInfo context for the current query level (currently unused but maintained for consistency)
- `tlist`: Target list of the subquery whose columns need Var representation
- `varno`: The RTE index (varno) that the generated Var nodes should reference

## Dependencies
- Functions called/Symbols referenced:
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [convert_ANY_sublink_to_join](../c/convert_ANY_sublink_to_join.md)

## Notes and Other Information
- The function is static, accessible only within subselect.c
- Primarily used in sublink-to-join conversion optimizations
- Only processes non-resjunk entries from the target list
- The varno parameter specifies which range table entry the Vars should reference
- Generated Var nodes preserve the type information from the original target entries
- Much simpler than generate_subquery_params as it doesn't need to create new parameters
- Located in src/backend/optimizer/plan/subselect.c:613-641