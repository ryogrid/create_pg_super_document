# create_scan_plan

## Location
[src/backend/optimizer/plan/createplan.c:560-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L560-L825)

## Overview
Creates scan plans for relation access by extracting restriction clauses, building target lists, and delegating to specialized scan plan creation functions based on the path type.

## Definition
```c
static Plan *create_scan_plan(PlannerInfo *root, Path *best_path, int flags)
```

## Detailed Description
create_scan_plan serves as the central dispatcher for creating all types of scan plans in PostgreSQL. It handles the common logic for scan plan creation including extracting and processing restriction clauses, handling parameterized scans, determining appropriate target lists, and managing gating clauses for pseudoconstant conditions. The function distinguishes between different scan types and delegates to specialized creation functions while optimizing target list generation through the use of physical target lists when beneficial.

The function handles both base relation scans and join-replacement scans (for ForeignScan and CustomScan), applies different clause extraction strategies for index scans versus other scan types, and manages the addition of gating Result nodes when pseudoconstant clauses are present.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: The path node representing the chosen scan strategy to be converted into a plan
- `flags`: Control flags affecting target list generation and labeling behavior (CP_IGNORE_TLIST, CP_LABEL_TLIST, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [use_physical_tlist](../u/use_physical_tlist.md)
  - [build_physical_tlist](../b/build_physical_tlist.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [get_gating_quals](../g/get_gating_quals.md)
  - [apply_pathtarget_labeling_to_tlist](../a/apply_pathtarget_labeling_to_tlist.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - copyObject
  - [create_seqscan_plan](create_seqscan_plan.md)
  - [create_samplescan_plan](create_samplescan_plan.md)
  - [create_indexscan_plan](create_indexscan_plan.md)
  - [create_bitmap_scan_plan](create_bitmap_scan_plan.md)
  - [create_tidscan_plan](create_tidscan_plan.md)
  - [create_tidrangescan_plan](create_tidrangescan_plan.md)
  - [create_subqueryscan_plan](create_subqueryscan_plan.md)
  - [create_functionscan_plan](create_functionscan_plan.md)
  - [create_tablefuncscan_plan](create_tablefuncscan_plan.md)
  - [create_valuesscan_plan](create_valuesscan_plan.md)
  - [create_ctescan_plan](create_ctescan_plan.md)
  - [create_namedtuplestorescan_plan](create_namedtuplestorescan_plan.md)
  - [create_resultscan_plan](create_resultscan_plan.md)
  - [create_worktablescan_plan](create_worktablescan_plan.md)
  - [create_foreignscan_plan](create_foreignscan_plan.md)
  - [create_customscan_plan](create_customscan_plan.md)
  - [create_gating_plan](create_gating_plan.md)
  - IS_JOIN_REL (macro)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- For IndexScan and IndexOnlyScan, uses indrestrictinfo instead of baserestrictinfo to avoid redundant predicate checks
- Handles parameterized scans by adding join clauses from outer relations to the scan clauses
- Optimizes target list generation by preferring physical target lists when possible to enable executor tuple projection optimization
- For IndexOnlyScan, uses the index's target list instead of building a physical one
- Supports both base relations and join-replacement scans (foreign/custom scans)
- Automatically adds gating Result nodes for pseudoconstant qualification evaluation
- Falls back to regular target list building when physical target list generation fails due to dropped columns
- Located at src/backend/optimizer/plan/createplan.c:560-825

## Simplified Source

```c
static Plan *
create_scan_plan(PlannerInfo *root, Path *best_path, int flags)
{
    RelOptInfo *rel = best_path->parent;
    List *scan_clauses;
    List *gating_clauses;
    List *tlist;
    Plan *plan;

    // Extract restriction clauses based on path type
    // For index scans, use predicate-filtered clauses
    if (best_path->pathtype == T_IndexScan || best_path->pathtype == T_IndexOnlyScan)
        scan_clauses = castNode(IndexPath, best_path)->indexinfo->indrestrictinfo;
    else
        scan_clauses = rel->baserestrictinfo;

    // Add parameterized join clauses if needed
    if (best_path->param_info)
        scan_clauses = list_concat_copy(scan_clauses, best_path->param_info->ppi_clauses);

    // Handle pseudoconstant qualifications
    gating_clauses = get_gating_quals(root, scan_clauses);
    if (gating_clauses)
        flags = 0;

    // Build optimal target list
    if (flags == CP_IGNORE_TLIST)
        tlist = NULL;
    else if (use_physical_tlist(root, best_path, flags))
        tlist = build_optimized_tlist(root, best_path, flags);
    else
        tlist = build_path_tlist(root, best_path);

    // Dispatch to specific scan plan creator based on path type
    switch (best_path->pathtype)
    {
        case T_SeqScan:
            plan = (Plan *) create_seqscan_plan(root, best_path, tlist, scan_clauses);
            break;
        case T_IndexScan:
            plan = (Plan *) create_indexscan_plan(root, (IndexPath *) best_path,
                                                  tlist, scan_clauses, false);
            break;
        case T_BitmapHeapScan:
            plan = (Plan *) create_bitmap_scan_plan(root, (BitmapHeapPath *) best_path,
                                                    tlist, scan_clauses);
            break;
        // ... other scan types handled similarly
        default:
            elog(ERROR, "unrecognized node type: %d", (int) best_path->pathtype);
            plan = NULL;
            break;
    }

    // Add gating Result node for pseudoconstant clauses if needed
    if (gating_clauses)
        plan = create_gating_plan(root, best_path, plan, gating_clauses);

    return plan;
}
```