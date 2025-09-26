# get_switched_clauses

## Location
[src/backend/optimizer/plan/createplan.c:5239-5315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5239-L5315)

## Overview
Extracts and rearranges join clauses to ensure the outer join variable is on the left and the inner variable is on the right, creating a modified list of clauses for merge or hash joins.

## Definition
```c
static List *get_switched_clauses(List *clauses, Relids outerrelids)
```

## Detailed Description
This function processes a list of merge or hash join clauses (represented as RestrictInfo nodes) and creates a modified list where each clause is oriented correctly for the join operation. The function ensures that the outer relation's variable appears on the left side of each clause and the inner relation's variable on the right side.

When a clause needs reordering (when the right side references the outer relation), the function creates a shallow copy of the OpExpr structure and uses CommuteOpExpr to swap the operands. This approach avoids deep copying the entire structure while preserving the original clause data. The function also sets the transient outer_is_left field in each RestrictInfo to track which side was which after processing.

## Parameters / Member Variables
- `clauses`: List of RestrictInfo nodes containing the join clauses to process
- `outerrelids`: Bitmapset identifying which relations are considered "outer" for join orientation

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - makeNode
  - [list_copy](../l/list_copy.md)
  - [CommuteOpExpr](../C/CommuteOpExpr.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md)

## Notes and Other Information
- The function performs shallow copying rather than deep copying for efficiency when commuting clauses
- Sets the opfuncid to InvalidOid in copied structures, which will be resolved later during execution
- The outer_is_left field in RestrictInfo is marked as transient, indicating it's used temporarily during plan creation
- Used specifically for merge and hash join planning where clause orientation matters for efficient join execution
- The function assumes all input clauses are operator expressions (OpExpr nodes)