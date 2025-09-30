# make_one_rel

## Location
[src/backend/optimizer/path/allpaths.c:171-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L171-L246)

## Overview
The main entry point for finding all possible access paths for executing a query, returning a single RelOptInfo that represents the join of all base relations in the query.

## Definition

```c
RelOptInfo *
make_one_rel(PlannerInfo *root, List *joinlist)
```
## Detailed Description
This function orchestrates the core path generation process in PostgreSQL's query optimizer. It coordinates several phases of optimization:
1. Marks base relations for startup cost consideration
2. Computes size estimates and parallel processing flags for base relations
3. Calculates the total table pages across all relations
4. Generates access paths for individual base relations
5. Constructs access paths for the complete join tree

The function ensures that all base relations and outer-join relations in the query are properly joined and returns a single RelOptInfo representing the entire query's join structure.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and query information
- : List structure representing the join tree structure to be optimized

## Dependencies
- Functions called/Symbols referenced:
  - [set_base_rel_consider_startup](../s/set_base_rel_consider_startup.md)
  - [set_base_rel_sizes](../s/set_base_rel_sizes.md)
  - [set_base_rel_pathlists](../s/set_base_rel_pathlists.md)
  - [make_rel_from_joinlist](make_rel_from_joinlist.md)
  - IS_DUMMY_REL (macro)
  - IS_SIMPLE_REL (macro)
  - [bms_equal](../b/bms_equal.md)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- Located in src/backend/optimizer/path/allpaths.c:171-246
- The function includes logic to calculate total_table_pages by iterating through all base relations
- Contains safeguards against double-counting appendrels (parent relations have pages = 0)
- Has a known limitation with self-joins being counted multiple times
- Includes assertion to verify the result joins all and only the query's base + outer-join relations
- Critical function in the PostgreSQL query optimization pipeline

## Simplified Source

```c
RelOptInfo *make_one_rel(PlannerInfo *root, List *joinlist) {
    RelOptInfo *rel;
    Index rti;
    double total_pages;

    // Configure base relations for startup cost considerations
    set_base_rel_consider_startup(root);

    // Compute size estimates and parallel flags for base relations
    set_base_rel_sizes(root);

    // Calculate total pages across all base relations
    total_pages = 0;
    for (rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo *base_rel = root->simple_rel_array[rti];

        if (base_rel == NULL || IS_DUMMY_REL(base_rel))
            continue;

        if (IS_SIMPLE_REL(base_rel))
            total_pages += (double) base_rel->pages;
    }
    root->total_table_pages = total_pages;

    // Generate access paths for each base relation
    set_base_rel_pathlists(root);

    // Generate access paths for the complete join tree
    rel = make_rel_from_joinlist(root, joinlist);

    return rel;
}
```