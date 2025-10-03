# create_index_paths

## Location
[src/backend/optimizer/path/indxpath.c:234-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L234-L430)

## Overview
Generates all interesting index paths for a given relation, including both plain (non-parameterized) and parameterized index scans, as well as bitmap heap paths for optimal query execution.

## Definition

```c
void
create_index_paths(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function is a core component of PostgreSQL's query optimizer that systematically examines all available indexes on a relation and generates appropriate access paths. It handles two fundamental types of index scans:

1. **Plain index scans**: Use only restriction clauses in their indexqual and can be applied in any context
2. **Parameterized index scans**: Use join clauses (plus restriction clauses) in their indexqual and must appear as the inner relation of a nestloop join

The function processes each index by:
- Matching restriction clauses to create non-parameterized paths
- Matching join clauses and EquivalenceClasses to create parameterized paths  
- Generating bitmap index paths for OR clauses
- Creating optimal BitmapHeapPaths by combining multiple bitmap index paths

All generated paths are added to the relation's pathlist via add_path() for cost-based selection by the optimizer.

## Parameters / Member Variables
- `*root`: PlannerInfo containing query planning context and global information
- `*rel`: RelOptInfo for the relation to generate index paths for (must have check_index_predicates() run previously)
## Dependencies
- Functions called/Symbols referenced:
  - [match_restriction_clauses_to_index](../m/match_restriction_clauses_to_index.md)
  - [get_index_paths](../g/get_index_paths.md)
  - [match_join_clauses_to_index](../m/match_join_clauses_to_index.md)
  - [match_eclass_clauses_to_index](../m/match_eclass_clauses_to_index.md)
  - [consider_index_join_clauses](consider_index_join_clauses.md)
  - [generate_bitmap_or_paths](../g/generate_bitmap_or_paths.md)
  - [choose_bitmap_and](choose_bitmap_and.md)
  - [create_bitmap_heap_path](create_bitmap_heap_path.md)
  - [create_partial_bitmap_paths](create_partial_bitmap_paths.md)
  - [add_path](../a/add_path.md)
- Called from (representative examples):
  - [set_plain_rel_pathlist](../s/set_plain_rel_pathlist.md)

## Notes and Other Information
- Skips processing if the relation has no indexes (rel->indexlist == NIL)
- Ignores partial indexes that don't match the query predicate (!index->predOK)
- Handles LATERAL references by including lateral_relids in path parameterization
- Creates parallel bitmap heap paths when appropriate (rel->consider_parallel)
- Uses IndexClauseSet structures to organize clauses by index column
- Generates only one BitmapHeapPath per distinct parameterization to avoid exponential path explosion

## Simplified Source

```c
void
create_index_paths(PlannerInfo *root, RelOptInfo *rel)
{
    List *bitindexpaths = NIL;
    List *bitjoinpaths = NIL;
    IndexClauseSet rclauseset, jclauseset, eclauseset;
    ListCell *lc;

    // Skip if no indexes available
    if (rel->indexlist == NIL)
        return;

    // Process each index
    foreach(lc, rel->indexlist)
    {
        IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

        // Skip partial indexes that don't match query
        if (index->indpred != NIL && !index->predOK)
            continue;

        // Find restriction clauses that match this index
        MemSet(&rclauseset, 0, sizeof(rclauseset));
        match_restriction_clauses_to_index(root, index, &rclauseset);

        // Create non-parameterized index paths
        get_index_paths(root, rel, index, &rclauseset, &bitindexpaths);

        // Find join clauses that match this index
        MemSet(&jclauseset, 0, sizeof(jclauseset));
        match_join_clauses_to_index(root, rel, index, &jclauseset, &joinorclauses);

        // Find EquivalenceClass clauses
        MemSet(&eclauseset, 0, sizeof(eclauseset));
        match_eclass_clauses_to_index(root, index, &eclauseset);

        // Create parameterized paths if join/eclass clauses found
        if (jclauseset.nonempty || eclauseset.nonempty)
            consider_index_join_clauses(root, rel, index,
                                       &rclauseset, &jclauseset, &eclauseset,
                                       &bitjoinpaths);
    }

    // Generate bitmap OR paths and create final bitmap heap paths
    // (Additional bitmap processing logic simplified)

    if (bitindexpaths != NIL)
    {
        Path *bitmapqual = choose_bitmap_and(root, rel, bitindexpaths);
        BitmapHeapPath *bpath = create_bitmap_heap_path(root, rel, bitmapqual,
                                                       rel->lateral_relids, 1.0, 0);
        add_path(rel, (Path *) bpath);
    }
}
```