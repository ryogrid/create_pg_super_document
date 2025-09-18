# generate_setop_child_grouplist

## Location
[src/backend/optimizer/plan/planner.c:8171-8214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L8171-L8214)

## Overview
Builds a SortGroupClause list defining the sort/grouping properties for a child of a set operation, adapting existing target entry references to match the set operation's requirements.

## Definition


## Detailed Description
This function creates a SortGroupClause list for a child query of a set operation (UNION, INTERSECT, EXCEPT) by adapting the parent set operation's grouping clauses to work with the child's target list. Unlike generate_setop_grouplist(), this function must handle cases where target list entries may already have tleSortGroupRef values assigned for other purposes like existing GROUP BY operations.

The function performs type compatibility checking between the child's target list and the set operation's expected column types. If any target list entry's type doesn't match the corresponding setop column type, the function returns an empty list, indicating that the child cannot be used with the set operation's grouping requirements.

For valid matches, the function preserves the original order of the set operation's group clauses while updating the tleSortGroupRef values to reference the appropriate target entries in the child's target list.

## Parameters / Member Variables
- : SetOperationStmt containing the parent set operation's grouping clauses and column type information
- : List of TargetEntry nodes from the child query whose grouping properties need to be defined

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - list_head
  - [lnext](../l/lnext.md)
  - exprType
  - [assignSortGroupRef](../a/assignSortGroupRef.md)
- Called from (representative examples):
  - [standard_qp_callback](../s/standard_qp_callback.md)

## Notes and Other Information
- The function skips resjunk columns as they may have sortgroupref values for internal purposes
- Type checking is strict - any type mismatch between child target entries and setop column types causes immediate failure
- Existing tleSortGroupRef assignments in target entries are preserved when possible through assignSortGroupRef
- Some target entries may end up with unreferenced ressortgroupref markings after processing, but this is harmless
- The function maintains the same order as the original set operation's group clauses for consistency
- Returns NIL if type mismatches are detected, allowing the caller to handle incompatible child queries appropriately