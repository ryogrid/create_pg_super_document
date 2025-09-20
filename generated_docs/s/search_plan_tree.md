# search_plan_tree

## Location
[src/backend/executor/execCurrent.c:314-426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execCurrent.c#L314-L426)

## Overview
Searches through a PlanState execution tree to find a scan node for a specified table, ensuring the found scan produced the current output row.

## Definition

```c
static ScanState *
search_plan_tree(PlanState *node, Oid table_oid,
				 bool *pending_rescan)
```
## Detailed Description
This recursive function traverses PostgreSQL's execution plan tree to locate a scan node that is scanning the specified table and is responsible for the plan tree's current output row. Unlike a simple search, it must ensure that any found scan actually contributed to the current execution state.

The function handles different node types with specific logic:

**Relation scan nodes** (SeqScan, IndexScan, BitmapHeapScan, etc.): Checks if the scan's current relation matches the target table OID. For ForeignScan and CustomScan nodes, it only considers those that have a currentRelation.

**AppendState nodes**: Recursively searches all input plans since only the plan that produced the current output could be positioned on a tuple. However, it rejects cases with multiple matches (which could occur with UNION ALL).

**Safe pass-through nodes** (ResultState, LimitState): These always return their input's current row, so the function safely descends through them.

**SubqueryScan nodes**: Special handling since the child plan is stored in the subplan field rather than as an outer plan.

The function tracks pending rescans by checking the chgParam field of nodes. If any node in the path to a found scan has chgParam set, it indicates a pending parameter change that would invalidate the current tuple position.

## Parameters / Member Variables
- : Current PlanState node being examined in the tree traversal
- : OID of the target table to find a scan for
- : Output flag set to true if a rescan is pending for the found scan

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine the specific node type)
  - [ScanState](../S/ScanState.md) (scan node state structure)
  - [AppendState](../A/AppendState.md) (append node state structure)
  - [SubqueryScanState](../S/SubqueryScanState.md) (subquery scan node state)
  - outerPlanState (macro to access outer plan state)
  - RelationGetRelid (to get relation OID from Relation)
  - [search_plan_tree](search_plan_tree.md) (recursive self-calls)
- Called from (representative examples):
  - [execCurrentOf](../e/execCurrentOf.md) (to find scan nodes in non-FOR-UPDATE cases)
  - [search_plan_tree](search_plan_tree.md) (recursive calls within the same function)

## Notes and Other Information
The function is marked static as it's only used within execCurrent.c. It returns NULL if no candidate is found or if multiple candidates are discovered (indicating ambiguity). The pending_rescan mechanism helps the caller determine if a found scan node's current position should be trusted. The function deliberately does not descend through certain node types like MergeAppend because their multiple active inputs make it impossible to determine which input produced the current output tuple. This conservative approach ensures correctness for CURRENT OF operations while maintaining reasonable performance.