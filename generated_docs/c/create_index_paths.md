# create_index_paths

## Location
src/backend/optimizer/path/indxpath.c: 234 - 430

## Overview
Generates all interesting index paths for a given relation, including both plain (non-parameterized) and parameterized index scans, as well as bitmap heap paths for optimal query execution.

## Definition


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
- : PlannerInfo containing query planning context and global information
- : RelOptInfo for the relation to generate index paths for (must have check_index_predicates() run previously)

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