# create_bitmap_scan_plan

## Location
[src/backend/optimizer/plan/createplan.c:3202-3331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L3202-L3331)

## Overview
Creates a bitmap heap scan plan node that combines bitmap index scans with heap tuple retrieval for efficient multi-index and complex condition processing.

## Definition
```c
static BitmapHeapScan *
create_bitmap_scan_plan(PlannerInfo *root,
                        BitmapHeapPath *best_path,
                        List *tlist,
                        List *scan_clauses)
```

## Detailed Description
The `create_bitmap_scan_plan` function creates a `BitmapHeapScan` plan node, which implements a two-phase scanning strategy: first, one or more bitmap index scans create a bitmap of qualifying tuple locations, then a heap scan retrieves the actual tuples from those locations. This approach is particularly efficient for:

1. **Multi-index queries**: Combining results from multiple indexes using AND/OR operations
2. **Low selectivity conditions**: When index scans would return many tuples, bitmap scans can reduce random I/O by sorting heap access
3. **Complex boolean expressions**: Efficiently handling combinations of indexed conditions

Key processing steps include:
- Converting the bitmap qualification tree into executable plan nodes via `create_bitmap_subplan`
- Determining which scan clauses need runtime checking (qpqual) versus those handled by index operations
- Handling parallel-aware execution for shared bitmap operations
- Processing parameterized paths and nested loop parameters
- Optimizing qualification checking by eliminating duplicates between qpqual and bitmapqualorig

The function includes sophisticated logic to minimize redundant condition checking, similar to `create_indexscan_plan` but adapted for bitmap scan semantics.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context information
- `best_path`: BitmapHeapPath representing the chosen bitmap scan path with cost estimates and bitmap qualification tree
- `tlist`: Target list specifying which columns/expressions should be returned by the scan  
- `scan_clauses`: List of RestrictInfo nodes representing WHERE clause conditions to be applied

## Dependencies
- Functions called/Symbols referenced:
  - [create_bitmap_subplan](create_bitmap_subplan.md)
  - [bitmap_subplan_mark_shared](../b/bitmap_subplan_mark_shared.md)
  - [list_member](../l/list_member.md)
  - [list_member_ptr](../l/list_member_ptr.md)
  - [contain_mutable_functions](contain_mutable_functions.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [list_difference_ptr](../l/list_difference_ptr.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_bitmap_heapscan](../m/make_bitmap_heapscan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - [BitmapHeapPath](../B/BitmapHeapPath.md), BitmapHeapScan (struct types)
  - RTE_RELATION (enum value)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- Bitmap scans provide a middle ground between sequential scans and index scans for moderate selectivity queries
- The two-phase approach (bitmap creation + heap scan) reduces random I/O compared to direct index scans
- Supports parallel execution through bitmap sharing among worker processes
- Includes predicate rechecking capabilities for lossy bitmap operations when memory is constrained
- The qualification logic differs from regular index scans because bitmap scans must handle predicate conditions in bitmapqualorig for potential lossy rechecking
- Eliminates duplicate conditions between qpqual and bitmapqualorig to avoid redundant runtime checks
- Particularly effective for queries with multiple AND/OR index conditions that would be expensive to evaluate with separate index scans

## Simplified Source

```c
static BitmapHeapScan *
create_bitmap_scan_plan(PlannerInfo *root, BitmapHeapPath *best_path,
                       List *tlist, List *scan_clauses) {
    Index baserelid = best_path->path.parent->relid;
    Plan *bitmapqualplan;
    List *bitmapqualorig, *indexquals, *indexECs, *qpqual;

    // Validate this is a base relation
    Assert(baserelid > 0);
    Assert(best_path->path.parent->rtekind == RTE_RELATION);

    // Convert bitmap qualification tree into executable plan
    bitmapqualplan = create_bitmap_subplan(root, best_path->bitmapqual,
                                          &bitmapqualorig, &indexquals, &indexECs);

    // Mark for parallel execution if needed
    if (best_path->path.parallel_aware)
        bitmap_subplan_mark_shared(bitmapqualplan);

    // Determine which scan clauses need runtime checking (qpqual)
    // Exclude conditions already handled by index operations
    qpqual = NIL;
    foreach(l, scan_clauses) {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);
        Node *clause = (Node *) rinfo->clause;

        // Skip pseudoconstants, duplicates, and implied conditions
        if (rinfo->pseudoconstant)
            continue;
        if (list_member(indexquals, clause))
            continue;
        if (rinfo->parent_ec && list_member_ptr(indexECs, rinfo->parent_ec))
            continue;
        if (!contain_mutable_functions(clause) &&
            predicate_implied_by(list_make1(clause), indexquals, false))
            continue;

        qpqual = lappend(qpqual, rinfo);
    }

    // Optimize qualification order and extract clauses
    qpqual = order_qual_clauses(root, qpqual);
    qpqual = extract_actual_clauses(qpqual, false);

    // Remove duplicates between qpqual and bitmapqualorig
    bitmapqualorig = list_difference_ptr(bitmapqualorig, qpqual);

    // Replace outer relation variables with nestloop params if needed
    if (best_path->path.param_info) {
        qpqual = (List *) replace_nestloop_params(root, (Node *) qpqual);
        bitmapqualorig = (List *) replace_nestloop_params(root, (Node *) bitmapqualorig);
    }

    // Create the final bitmap heap scan plan node
    BitmapHeapScan *scan_plan = make_bitmap_heapscan(tlist, qpqual, bitmapqualplan,
                                                    bitmapqualorig, baserelid);
    copy_generic_path_info(&scan_plan->scan.plan, &best_path->path);

    return scan_plan;
}
```