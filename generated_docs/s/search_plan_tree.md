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
- `*node`: Current PlanState node being examined in the tree traversal
- `table_oid`: OID of the target table to find a scan for
- `*pending_rescan`: Output flag set to true if a rescan is pending for the found scan
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

## Simplified Source

```c
static ScanState *
search_plan_tree(PlanState *node, Oid table_oid, bool *pending_rescan)
{
    ScanState *result = NULL;

    if (node == NULL)
        return NULL;

    switch (nodeTag(node)) {
        // Handle all scan node types that can scan relations
        case T_SeqScanState:
        case T_SampleScanState:
        case T_IndexScanState:
        case T_IndexOnlyScanState:
        case T_BitmapHeapScanState:
        case T_TidScanState:
        case T_TidRangeScanState:
        case T_ForeignScanState:
        case T_CustomScanState:
        {
            ScanState *scan_state = (ScanState *) node;

            // Check if this scan node is scanning our target table
            if (scan_state->ss_currentRelation &&
                RelationGetRelid(scan_state->ss_currentRelation) == table_oid)
                result = scan_state;
            break;
        }

        // Append nodes: search all input plans, reject if multiple matches
        case T_AppendState:
        {
            AppendState *append_state = (AppendState *) node;

            for (int i = 0; i < append_state->as_nplans; i++) {
                ScanState *candidate = search_plan_tree(append_state->appendplans[i],
                                                       table_oid, pending_rescan);
                if (!candidate)
                    continue;
                if (result)
                    return NULL;  // Multiple matches - ambiguous
                result = candidate;
            }
            break;
        }

        // Pass-through nodes: safe to descend since they return input's current row
        case T_ResultState:
        case T_LimitState:
            result = search_plan_tree(outerPlanState(node), table_oid, pending_rescan);
            break;

        // SubqueryScan: child is in subplan field
        case T_SubqueryScanState:
            result = search_plan_tree(((SubqueryScanState *) node)->subplan,
                                    table_oid, pending_rescan);
            break;

        default:
            // Other node types: don't descend (unsafe or unknown)
            break;
    }

    // If we found a scan and this node has pending parameter changes,
    // mark that a rescan will be needed
    if (result && node->chgParam != NULL)
        *pending_rescan = true;

    return result;
}
```