# set_plan_refs

## Location
[src/backend/optimizer/plan/setrefs.c:608-1320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L608-L1320)

## Overview
The core recursive function that adjusts variable references and expression nodes within a Plan tree to account for rangetable (RT) index offsets when integrating subqueries into larger query plans.

## Definition

```c
static Plan *
set_plan_refs(PlannerInfo *root, Plan *plan, int rtoffset)
```
## Detailed Description
 is the main workhorse function in PostgreSQL's plan reference adjustment system. It recursively traverses a Plan node tree and updates variable references to account for rangetable index offsets that occur when subqueries are integrated into larger query plans. Each plan node type requires different handling based on its structure and the expressions it contains.

The function operates by:
1. Assigning a unique plan node ID to each node
2. Performing plan-type-specific reference adjustments via a large switch statement
3. Recursively processing child plans (lefttree and righttree)

Key behaviors include:
- Scan nodes: Updates scanrelid and fixes targetlist/qual expressions using 
- Join nodes: Delegates to  for complex join expression handling
- Upper nodes: Uses  for aggregation and window operations
- Special nodes: IndexOnlyScan and SubqueryScan get specialized treatment via dedicated functions

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planner state and context
- `*plan`: The Plan node to process and adjust references for
- `rtoffset`: Integer offset to add to rangetable indices for proper variable resolution
## Dependencies
- Functions called/Symbols referenced:
  - fix_scan_list: Fixes variable references in expression lists for scan operations
  - [fix_scan_expr](../f/fix_scan_expr.md): Fixes variable references in individual expressions
  - [set_indexonlyscan_references](set_indexonlyscan_references.md): Specialized handling for index-only scans
  - [set_subqueryscan_references](set_subqueryscan_references.md): Specialized handling for subquery scans
  - [set_join_references](set_join_references.md): Handles complex join expression reference adjustment
  - [set_upper_references](set_upper_references.md): Processes upper-level plan nodes like aggregation
  - [set_dummy_tlist_references](set_dummy_tlist_references.md): Sets references for nodes that don't evaluate targetlists
  - NUM_EXEC_TLIST/NUM_EXEC_QUAL: Macros for determining execution context
- Called from (representative examples):
  - [set_plan_references](set_plan_references.md): Top-level entry point for plan reference adjustment
  - [set_plan_refs](set_plan_refs.md): Recursive self-calls for child plan processing
  - [set_append_references](set_append_references.md): When processing Append node children
  - [set_customscan_references](set_customscan_references.md): For custom scan node subplans

## Notes and Other Information
The function must process parent nodes before their children to ensure proper variable matching during reference adjustment. This top-down approach is critical because child nodes' variables must be matched against the already-adjusted expressions in parent nodes. The function handles over 30 different plan node types, each with specific requirements for reference adjustment, making it one of the most comprehensive functions in the PostgreSQL query planner.

## Simplified Source

```c
static Plan *set_plan_refs(PlannerInfo *root, Plan *plan, int rtoffset) {
    ListCell *l;

    if (plan == NULL) return NULL;

    // Assign unique plan node ID
    plan->plan_node_id = root->glob->lastPlanNodeId++;

    // Plan-type-specific reference fixes
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_SampleScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_TidRangeScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_NamedTuplestoreScan: {
            // Standard scan nodes: adjust scanrelid and fix expressions
            Scan *splan = (Scan *) plan;
            splan->scanrelid += rtoffset;
            splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset, NUM_EXEC_TLIST(plan));
            splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset, NUM_EXEC_QUAL(plan));
            // Handle node-specific expressions (indexqual, functions, etc.)
            break;
        }

        case T_IndexScan: {
            // Index scan: fix scan expressions plus index-specific qualifications
            IndexScan *splan = (IndexScan *) plan;
            splan->scan.scanrelid += rtoffset;
            fix_index_scan_expressions(splan, root, rtoffset);
            break;
        }

        case T_IndexOnlyScan:
            return set_indexonlyscan_references(root, (IndexOnlyScan *) plan, rtoffset);

        case T_SubqueryScan:
            return set_subqueryscan_references(root, (SubqueryScan *) plan, rtoffset);

        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
            // Join operations: delegate to specialized join handler
            set_join_references(root, (Join *) plan, rtoffset);
            break;

        case T_Agg: {
            // Aggregation: handle combining operations and set upper references
            Agg *agg = (Agg *) plan;
            if (DO_AGGSPLIT_COMBINE(agg->aggsplit)) {
                convert_combining_aggrefs_in_plan(plan);
            }
            set_upper_references(root, plan, rtoffset);
            break;
        }

        case T_Material:
        case T_Sort:
        case T_Unique: {
            // Nodes that don't evaluate targetlists - fix for EXPLAIN
            set_dummy_tlist_references(plan, rtoffset);
            Assert(plan->qual == NIL);
            break;
        }

        case T_ModifyTable: {
            // Complex DML operations: handle RETURNING, ON CONFLICT, MERGE
            ModifyTable *splan = (ModifyTable *) plan;
            fix_modifytable_references(root, splan, rtoffset);
            break;
        }

        case T_Append:
            return set_append_references(root, (Append *) plan, rtoffset);

        case T_WindowAgg:
        case T_Group:
        case T_Result:
        case T_Limit:
            // Upper nodes with specific expression handling
            handle_upper_node_references(root, plan, rtoffset);
            break;

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(plan));
    }

    // Recursively process child plans AFTER adjusting this node
    plan->lefttree = set_plan_refs(root, plan->lefttree, rtoffset);
    plan->righttree = set_plan_refs(root, plan->righttree, rtoffset);

    return plan;
}
```