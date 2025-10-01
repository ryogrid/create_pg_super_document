# create_unique_path

## Location
[src/backend/optimizer/util/pathnode.c:1654-1880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1654-L1880)

## Overview
Creates a UniquePath node that represents elimination of duplicate rows from input data, with distinctness defined according to semijoin requirements, using either sorting or hashing strategies.

## Definition
```c
UniquePath *create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                              SpecialJoinInfo *sjinfo)
```

## Detailed Description
The `create_unique_path` function constructs a UniquePath node that corresponds to a Unique plan node in PostgreSQL's query execution. This operation eliminates duplicate rows from the input data based on the distinctness requirements of a semijoin operation. The function is intelligent about choosing the best uniqueness strategy and can even optimize away the operation entirely if the input is already proven to be unique.

The function implements several optimization strategies:
1. **NOOP optimization**: If the input relation has a unique index covering the required columns, or if it's a subquery that already guarantees uniqueness, no actual unique operation is needed.
2. **Sort-based uniqueness**: Sorts the input data and then removes adjacent duplicates.
3. **Hash-based uniqueness**: Uses a hash table to track seen values and eliminate duplicates.

The function caches its result in the relation's cheapest_unique_path field since it's likely to be called repeatedly with the same parameters during join planning.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `rel`: The RelOptInfo representing the relation that needs to be made unique
- `subpath`: The input Path (must be the cheapest_total_path for the relation)
- `sjinfo`: SpecialJoinInfo structure containing semijoin details that define uniqueness requirements

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the UniquePath node)
  - copyObject (to copy semijoin operators and expressions)
  - [relation_has_unique_index_for](../r/relation_has_unique_index_for.md) (to check for existing unique indexes)
  - [query_supports_distinctness](../q/query_supports_distinctness.md)/query_is_distinct_for (to check subquery uniqueness)
  - [translate_sub_tlist](../t/translate_sub_tlist.md) (to translate subquery target list columns)
  - [estimate_num_groups](../e/estimate_num_groups.md) (to estimate output row count)
  - [cost_sort](cost_sort.md) (to cost sort-based uniqueness strategy)
  - [cost_agg](cost_agg.md) (to cost hash-based uniqueness strategy)
  - [bms_equal](../b/bms_equal.md) (to compare relation ID sets)
- Called from (representative examples):
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md) (during sort-merge join planning)
  - [match_unsorted_outer](../m/match_unsorted_outer.md) (during nested loop join planning)
  - [hash_inner_and_outer](../h/hash_inner_and_outer.md) (during hash join planning)
  - [join_is_legal](../j/join_is_legal.md) (when checking join legality for semijoins)

## Notes and Other Information
- The function asserts that the subpath is the cheapest_total_path and that the SpecialJoinInfo represents a semijoin
- Results are cached in rel->cheapest_unique_path to avoid redundant computation
- Memory allocation is carefully managed to handle GEQO planning contexts appropriately
- The function can return NULL if uniqueness cannot be achieved with available methods
- [Hash](../H/Hash.md)-based uniqueness is abandoned if the estimated memory requirement exceeds hash_memory_limit
- The choice between sort and hash methods is made based on cost comparison when both are available
- For subqueries, the function leverages translate_sub_tlist to map expressions to subquery output columns
- The pathkeys of the result depend on the uniqueness method chosen (preserved for NOOP, cleared otherwise)

## Simplified Source

```c
UniquePath *create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                              SpecialJoinInfo *sjinfo) {
    UniquePath *pathnode;
    Path sort_path, agg_path;
    MemoryContext oldcontext;

    // Return cached result if available
    if (rel->cheapest_unique_path)
        return (UniquePath *) rel->cheapest_unique_path;

    // Return NULL if uniqueness is not possible
    if (!(sjinfo->semi_can_btree || sjinfo->semi_can_hash))
        return NULL;

    // Switch to appropriate memory context
    oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));

    // Create and initialize UniquePath node
    pathnode = makeNode(UniquePath);
    pathnode->path.pathtype = T_Unique;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;
    pathnode->subpath = subpath;
    pathnode->in_operators = copyObject(sjinfo->semi_operators);
    pathnode->uniq_exprs = copyObject(sjinfo->semi_rhs_exprs);

    // Optimization 1: Check if relation already has unique index
    if (rel->rtekind == RTE_RELATION && sjinfo->semi_can_btree &&
        relation_has_unique_index_for(root, rel, NIL,
                                     sjinfo->semi_rhs_exprs,
                                     sjinfo->semi_operators)) {
        pathnode->umethod = UNIQUE_PATH_NOOP;
        pathnode->path.rows = rel->rows;
        pathnode->path.startup_cost = subpath->startup_cost;
        pathnode->path.total_cost = subpath->total_cost;
        goto cache_and_return;
    }

    // Optimization 2: Check if subquery already guarantees uniqueness
    if (rel->rtekind == RTE_SUBQUERY) {
        RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
        if (query_supports_distinctness(rte->subquery)) {
            List *sub_tlist_colnos = translate_sub_tlist(sjinfo->semi_rhs_exprs, rel->relid);
            if (sub_tlist_colnos &&
                query_is_distinct_for(rte->subquery, sub_tlist_colnos, sjinfo->semi_operators)) {
                pathnode->umethod = UNIQUE_PATH_NOOP;
                pathnode->path.rows = rel->rows;
                pathnode->path.startup_cost = subpath->startup_cost;
                pathnode->path.total_cost = subpath->total_cost;
                goto cache_and_return;
            }
        }
    }

    // Estimate output rows
    pathnode->path.rows = estimate_num_groups(root, sjinfo->semi_rhs_exprs, rel->rows, NULL, NULL);

    // Cost sort-based approach if possible
    if (sjinfo->semi_can_btree) {
        cost_sort(&sort_path, root, NIL, subpath->total_cost, rel->rows,
                  subpath->pathtarget->width, 0.0, work_mem, -1.0);
        sort_path.total_cost += cpu_operator_cost * rel->rows * list_length(sjinfo->semi_rhs_exprs);
    }

    // Cost hash-based approach if possible
    if (sjinfo->semi_can_hash) {
        int hashentrysize = subpath->pathtarget->width + 64;
        if (hashentrysize * pathnode->path.rows > get_hash_memory_limit()) {
            sjinfo->semi_can_hash = false;
        } else {
            cost_agg(&agg_path, root, AGG_HASHED, NULL,
                     list_length(sjinfo->semi_rhs_exprs), pathnode->path.rows,
                     NIL, subpath->startup_cost, subpath->total_cost,
                     rel->rows, subpath->pathtarget->width);
        }
    }

    // Choose the best method
    if (sjinfo->semi_can_btree && sjinfo->semi_can_hash) {
        pathnode->umethod = (agg_path.total_cost < sort_path.total_cost) ?
                           UNIQUE_PATH_HASH : UNIQUE_PATH_SORT;
    } else if (sjinfo->semi_can_btree) {
        pathnode->umethod = UNIQUE_PATH_SORT;
    } else if (sjinfo->semi_can_hash) {
        pathnode->umethod = UNIQUE_PATH_HASH;
    } else {
        MemoryContextSwitchTo(oldcontext);
        return NULL;
    }

    // Set costs based on chosen method
    if (pathnode->umethod == UNIQUE_PATH_HASH) {
        pathnode->path.startup_cost = agg_path.startup_cost;
        pathnode->path.total_cost = agg_path.total_cost;
    } else {
        pathnode->path.startup_cost = sort_path.startup_cost;
        pathnode->path.total_cost = sort_path.total_cost;
    }

cache_and_return:
    rel->cheapest_unique_path = (Path *) pathnode;
    MemoryContextSwitchTo(oldcontext);
    return pathnode;
}
```