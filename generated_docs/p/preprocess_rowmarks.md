# preprocess_rowmarks

## Location
[src/backend/optimizer/plan/planner.c:2295-2406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2295-L2406)

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

## Simplified Source

```c
static void preprocess_rowmarks(PlannerInfo *root) {
    Query *parse = root->parse;
    Bitmapset *rels;
    List *prowmarks;

    // Check if we have explicit row marks (FOR UPDATE/SHARE)
    if (parse->rowMarks) {
        // Validate that row locking is compatible with query structure
        CheckSelectLocking(parse, linitial_node(RowMarkClause, parse->rowMarks)->strength);
    } else {
        // Only need row marks for UPDATE, DELETE, MERGE commands
        if (parse->commandType != CMD_UPDATE &&
            parse->commandType != CMD_DELETE &&
            parse->commandType != CMD_MERGE)
            return;
    }

    // Get all base relations except the target relation
    rels = get_relids_in_jointree((Node *) parse->jointree, false, false);
    if (parse->resultRelation)
        rels = bms_del_member(rels, parse->resultRelation);

    prowmarks = NIL;

    // Convert explicit RowMarkClauses to PlanRowMark structures
    foreach(l, parse->rowMarks) {
        RowMarkClause *rc = lfirst_node(RowMarkClause, l);
        RangeTblEntry *rte = rt_fetch(rc->rti, parse->rtable);
        PlanRowMark *newrc;

        // Skip non-relation entries (subqueries, etc.)
        if (rte->rtekind != RTE_RELATION)
            continue;

        rels = bms_del_member(rels, rc->rti);

        // Create PlanRowMark for explicit row lock
        newrc = makeNode(PlanRowMark);
        newrc->rti = newrc->prti = rc->rti;
        newrc->rowmarkId = ++(root->glob->lastRowMarkId);
        newrc->markType = select_rowmark_type(rte, rc->strength);
        newrc->strength = rc->strength;
        newrc->waitPolicy = rc->waitPolicy;

        prowmarks = lappend(prowmarks, newrc);
    }

    // Add row marks for remaining base relations (implicit locking)
    i = 0;
    foreach(l, parse->rtable) {
        RangeTblEntry *rte = lfirst_node(RangeTblEntry, l);
        PlanRowMark *newrc;

        i++;
        if (!bms_is_member(i, rels))
            continue;

        // Create PlanRowMark for implicit row lock
        newrc = makeNode(PlanRowMark);
        newrc->rti = newrc->prti = i;
        newrc->rowmarkId = ++(root->glob->lastRowMarkId);
        newrc->markType = select_rowmark_type(rte, LCS_NONE);
        newrc->strength = LCS_NONE;
        newrc->waitPolicy = LockWaitBlock;

        prowmarks = lappend(prowmarks, newrc);
    }

    root->rowMarks = prowmarks;
}
```