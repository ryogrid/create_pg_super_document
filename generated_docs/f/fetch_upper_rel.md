# fetch_upper_rel

## Location
[src/backend/optimizer/util/relnode.c:1470-1520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L1470-L1520)

## Overview
Builds or retrieves a RelOptInfo for post-scan/join query processing operations, known as "upper" relations.

## Definition

```c
RelOptInfo *fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind, Relids relids)
```
## Detailed Description
The  function manages RelOptInfo structures for upper-level query processing operations that occur after basic scanning and joining. These "upper" relations represent processing steps like grouping, windowing, ordering, and set operations.

The function first searches the existing upper_rels list for the specified kind to see if a matching relation already exists. If found, it returns the existing RelOptInfo. If not found, it creates a new RelOptInfo with appropriate initialization for upper-level processing.

Upper relations are identified by an UpperRelationKind enum value and a Relids set. The meaning of the Relids set varies depending on the specific relation kind. Most fields in upper-level RelOptInfo structures are not used and remain zero-initialized, with the function focusing only on fields relevant to path management.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning state
- `kind`: UpperRelationKind enum specifying the type of upper relation (grouping, window, etc.)
- `relids`: Relids set identifying the specific upper relation (meaning varies by kind)

## Dependencies
- Functions called/Symbols referenced:
  - UpperRelationKind
  - [bms_equal](../b/bms_equal.md)
  - RELOPT_UPPER_REL
  - [bms_copy](../b/bms_copy.md)
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md)
- Called from (representative examples):
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md)
  - [standard_planner](../s/standard_planner.md)
  - [subquery_planner](../s/subquery_planner.md)
  - [grouping_planner](../g/grouping_planner.md)
  - [make_grouping_rel](../m/make_grouping_rel.md)
  - [create_window_paths](../c/create_window_paths.md)
  - [create_distinct_paths](../c/create_distinct_paths.md)
  - [create_ordered_paths](../c/create_ordered_paths.md)

## Notes and Other Information
- The function uses a simple List-based indexing structure for each relation kind, which could be optimized if performance becomes an issue
- Only fields relevant to add_path() and set_cheapest() are properly initialized
- The consider_startup flag is set based on whether tuple_fraction > 0, indicating partial result retrieval
- Parallel processing consideration is initially disabled but may be enabled later
- The function operates at lines 1470-1520 in src/backend/optimizer/util/relnode.c
- This is a key function in PostgreSQL's upper-level query planning infrastructure

## Simplified Source

```c
RelOptInfo *
fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind, Relids relids)
{
    RelOptInfo *upperrel;
    ListCell   *lc;

    // Check if we already created this upper relation
    foreach(lc, root->upper_rels[kind])
    {
        upperrel = (RelOptInfo *) lfirst(lc);
        if (bms_equal(upperrel->relids, relids))
            return upperrel;
    }

    // Create new upper relation
    upperrel = makeNode(RelOptInfo);
    upperrel->reloptkind = RELOPT_UPPER_REL;
    upperrel->relids = bms_copy(relids);

    // Initialize path-related fields
    upperrel->consider_startup = (root->tuple_fraction > 0);
    upperrel->reltarget = create_empty_pathtarget();
    upperrel->pathlist = NIL;
    upperrel->cheapest_startup_path = NULL;
    upperrel->cheapest_total_path = NULL;

    // Add to relation list and return
    root->upper_rels[kind] = lappend(root->upper_rels[kind], upperrel);
    return upperrel;
}
```