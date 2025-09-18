# preprocess_rowmarks

## Location
src/backend/optimizer/plan/planner.c: 2295 - 2406

## Overview
Sets up PlanRowMark structures for row locking operations including FOR UPDATE/SHARE clauses and implicit locking needed for UPDATE/DELETE/MERGE operations.

## Definition
```c
static void preprocess_rowmarks(PlannerInfo *root)
```

## Detailed Description
This function is responsible for creating the PlanRowMark structures that control row locking behavior during query execution. It handles two main scenarios:

1. **Explicit Row Locking**: Processes FOR [KEY] UPDATE/SHARE clauses from the query
2. **Implicit Row Locking**: Adds necessary row marks for UPDATE/DELETE/MERGE operations on base relations

The function performs several key operations:
- **Validation**: Checks that row locking is compatible with the query structure (e.g., not used with grouping)
- **Base Relation Identification**: Determines which relations need row marks (all base relations except the target)
- **PlanRowMark Creation**: Converts RowMarkClauses to PlanRowMark structures with appropriate locking types
- **Lock Type Selection**: Uses select_rowmark_type to determine the appropriate locking mechanism for each relation
- **ID Assignment**: Assigns unique rowmarkId values for execution tracking

The function distinguishes between relations that have explicit FOR UPDATE/SHARE clauses and those that need implicit locking, creating PlanRowMark entries for both cases with appropriate lock strengths and types.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context (modified to set root->rowMarks)

## Dependencies
- Functions called/Symbols referenced:
  - [CheckSelectLocking](../C/CheckSelectLocking.md), get_relids_in_jointree, select_rowmark_type
  - [bms_del_member](../b/bms_del_member.md), bms_is_member, rt_fetch
  - makeNode (PlanRowMark), linitial_node (RowMarkClause)
  - CMD_UPDATE, CMD_DELETE, CMD_MERGE, RTE_RELATION, LCS_NONE, LockWaitBlock
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)

## Notes and Other Information
- Located in src/backend/optimizer/plan/planner.c:2295-2406
- This is a static function called during the preprocessing phase of query planning
- The function sets root->rowMarks as a side effect, which is used by later planning phases
- Row marks are not needed for SELECT queries unless they have explicit FOR UPDATE/SHARE clauses
- Subqueries and non-relation RTEs are handled specially - subqueries get ROW_MARK_COPY treatment
- The function assigns unique rowmarkId values using root->glob->lastRowMarkId
- Target relations (result relations) are excluded from row marking since they are handled differently
- Each PlanRowMark includes lock strength, wait policy, and mark type information for the executor