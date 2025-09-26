# create_modifytable_path

## Location
[src/backend/optimizer/util/pathnode.c:3725-3825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3725-L3825)

## Overview
Creates a pathnode that represents performing INSERT/UPDATE/DELETE/MERGE operations on database tables, serving as the top-level node for data modification queries in the query planner.

## Definition

```c
ModifyTablePath *
create_modifytable_path(PlannerInfo *root, RelOptInfo *rel,
						Path *subpath,
						CmdType operation, bool canSetTag,
						Index nominalRelation, Index rootRelation,
						bool partColsUpdated,
						List *resultRelations,
						List *updateColnosLists,
						List *withCheckOptionLists, List *returningLists,
						List *rowMarks, OnConflictExpr *onconflict,
						List *mergeActionLists, List *mergeJoinConditions,
						int epqParam)
```
## Detailed Description
This function creates a ModifyTablePath node that represents data modification operations (INSERT, UPDATE, DELETE, MERGE) in PostgreSQL's query planning system. It serves as the top-level path node for any query that modifies table data and wraps a subpath that produces the source data for the modification.

The function handles complex scenarios including partitioned tables, RETURNING clauses, WITH CHECK OPTIONS, ON CONFLICT handling, and MERGE operations. It validates that the provided lists have consistent lengths and sets up the path structure with appropriate cost estimates.

Cost calculation is intentionally simplified since ModifyTable is always a top-level node where cost differences don't affect higher-level planning decisions. The function sets parallel execution to false since data modification operations cannot be parallelized.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information
- : RelOptInfo representing the parent relation associated with the result
- : Path producing source data for the modification operation
- : CmdType specifying the operation (INSERT, UPDATE, DELETE, MERGE)
- : Boolean indicating if the command can set the command tag/es_processed
- : Parent RT index used for EXPLAIN output
- : Partitioned/inherited table root RTI, or 0 if none
- : Boolean indicating if any partitioning columns are being updated
- : Integer list of actual RT indexes of target relations
- : List of UPDATE target column number lists (one per relation)
- : List of WITH CHECK OPTION lists (one per relation)
- : List of RETURNING target lists (one per relation)
- : List of PlanRowMarks for non-locking operations
- : ON CONFLICT clause specification, or NULL
- : Parameter ID for EvalPlanQual re-evaluation
- : List of MERGE action lists (one per relation)
- : List of join conditions for MERGE operations (one per relation)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create ModifyTablePath node)
  - [list_length](../l/list_length.md) (for validating list consistency)
  - CmdType, CMD_MERGE, CMD_UPDATE (command type constants)
  - [OnConflictExpr](../O/OnConflictExpr.md) (ON CONFLICT expression structure)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:2009)

## Notes and Other Information
- Always creates a top-level path node that cannot be parallelized
- Cost estimation is simplified since it doesn't affect higher-level planning decisions
- Supports complex features like partitioning, RETURNING clauses, and MERGE operations
- Validates list length consistency between resultRelations and related parameter lists
- Row count is set to subpath rows if RETURNING is present, otherwise 0
- [Path](../P/Path.md) target width handling is acknowledged as suboptimal but maintained for historical compatibility
- No pathkeys (sort order) since modification operations don't preserve ordering