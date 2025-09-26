# PlanRowMark

## Location
src/include/nodes/plannodes.h: 1377 - 1390

## Overview
PlanRowMark represents plan-time information for FOR [KEY] UPDATE/SHARE clauses, specifying how rows should be marked or locked during query execution.

## Definition
```c
typedef struct PlanRowMark
{
    pg_node_attr(no_equal, no_query_jumble)

    NodeTag     type;
    Index       rti;            /* range table index of markable relation */
    Index       prti;           /* range table index of parent relation */
    Index       rowmarkId;      /* unique identifier for resjunk columns */
    RowMarkType markType;       /* see enum above */
    int         allMarkTypes;   /* OR of (1<<markType) for all children */
    LockClauseStrength strength;    /* LockingClause's strength, or LCS_NONE */
    LockWaitPolicy waitPolicy;  /* NOWAIT and SKIP LOCKED options */
    bool        isParent;       /* true if this is a "dummy" parent entry */
} PlanRowMark;
```

## Detailed Description
PlanRowMark structures define how rows should be marked or locked during query execution for UPDATE/DELETE/MERGE/SELECT FOR UPDATE/SHARE operations. When the planner discovers inheritance hierarchies, it creates a complex structure of PlanRowMark entries: parent entries with isParent=true, and child entries for each relation in the hierarchy.

The structure handles both regular tables and inheritance hierarchies. For inheritance, child entries have rti != prti (child's RT index vs parent's RT index), while parent entries serve as coordination points with allMarkTypes combining the mark types of all children.

The planner adds resjunk output columns to carry row identification information. For non-COPY mark types, these include tableoid%u and ctid%u columns. For ROW_MARK_COPY, a single wholerow%u column contains the entire row value. The %u represents the rowmarkId, which is unique within the plan tree and shared across inheritance hierarchies.

## Parameters / Member Variables
- `type`: NodeTag for type identification in PostgreSQL's node system
- `rti`: Range table index of the relation to be marked/locked
- `prti`: Range table index of the parent relation (equals rti for non-inheritance cases)
- `rowmarkId`: Unique identifier used in resjunk column names (tableoid%u, ctid%u, wholerow%u)
- `markType`: RowMarkType specifying the locking strategy (EXCLUSIVE, SHARE, REFERENCE, COPY, etc.)
- `allMarkTypes`: Bitmask combining all child mark types (1<<markType) for inheritance hierarchies
- `strength`: LockClauseStrength from the original FOR UPDATE/SHARE clause, or LCS_NONE
- `waitPolicy`: Lock wait policy (NOWAIT, SKIP LOCKED options from the query)
- `isParent`: True for dummy parent entries in inheritance hierarchies that coordinate child entries

## Dependencies
- Functions called/Symbols referenced:
  - RowMarkType (enumeration of row marking strategies)
  - LockClauseStrength (locking strength specifications)
  - LockWaitPolicy (wait policy for lock conflicts)
  - NodeTag (node type identification)
  - Index (range table indices)
- Called from (representative examples):
  - preprocess_rowmarks (planner preprocessing)
  - ExecInitLockRows (executor initialization)
  - ExecInitModifyTable (modify table executor setup)
  - set_plan_references (plan reference resolution)

## Notes and Other Information
- Initially all PlanRowMarks have rti == prti and isParent == false before inheritance processing
- Child relations in inheritance hierarchies can use different markTypes, with the parent's allMarkTypes field tracking all variants
- The rowmarkId numbering scheme avoids conflicts when flattening subqueries since resjunk column names don't need renumbering
- Relations not explicitly specified as FOR UPDATE/SHARE are marked as ROW_MARK_REFERENCE (regular tables) or ROW_MARK_COPY (others)
- For inheritance hierarchies, all tables share the same resjunk column names due to the shared rowmarkId
- The structure supports both early locking (during scan) and late locking (after row fetch) strategies depending on the relation type and FDW capabilities