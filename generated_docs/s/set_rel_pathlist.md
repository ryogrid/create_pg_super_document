# set_rel_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:469-571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L469-L571)

## Overview
Sets up access paths for a base relation by determining the appropriate pathlist generation strategy based on the relation type and characteristics.

## Definition

```c
static void
set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
				 Index rti, RangeTblEntry *rte)
```
## Detailed Description
This function is the central dispatcher for building access paths for base relations in PostgreSQL's query optimizer. It examines the relation type and characteristics to delegate path creation to specialized functions. The function handles various relation types including regular tables, foreign tables, subqueries, functions, CTEs, and more. It also manages plugin hooks and finalizes the pathlist by selecting the cheapest paths and optionally generating gather paths for parallel execution.

The function first checks if the relation is a dummy relation (already proven empty) and skips processing if so. For inheritance relations, it delegates to . For regular relations, it uses a switch statement to handle different  values, calling appropriate specialized pathlist functions.

After core path generation, the function allows plugins to modify the pathlist via , then considers generating gather paths for parallel execution (except for inheritance children and top-level relations), and finally determines the cheapest paths.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and context
- : RelOptInfo structure representing the relation for which to build paths
- : Range table index identifying the relation in the query's range table
- : RangeTblEntry containing metadata about the relation from the parse tree

## Dependencies
- Functions called/Symbols referenced:
  - [set_append_rel_pathlist](set_append_rel_pathlist.md) (for inheritance relations)
  - [set_foreign_pathlist](set_foreign_pathlist.md) (for foreign tables)
  - [set_tablesample_rel_pathlist](set_tablesample_rel_pathlist.md) (for sampled relations)
  - [set_plain_rel_pathlist](set_plain_rel_pathlist.md) (for regular tables)
  - [set_function_pathlist](set_function_pathlist.md) (for range functions)
  - [set_tablefunc_pathlist](set_tablefunc_pathlist.md) (for table functions)
  - [set_values_pathlist](set_values_pathlist.md) (for VALUES lists)
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md) (for parallel path generation)
  - [set_cheapest](set_cheapest.md) (to determine optimal paths)
  - IS_DUMMY_REL, bms_equal (utility macros/functions)
- Called from:
  - [set_base_rel_pathlists](set_base_rel_pathlists.md) (main entry point for base relation processing)
  - [set_append_rel_pathlist](set_append_rel_pathlist.md) (recursive call for inheritance hierarchies)

## Notes and Other Information
- This is a static function within allpaths.c, serving as an internal dispatcher
- The function supports plugin extensibility through 
- Parallel execution planning is carefully managed to avoid excessive gather nodes in inheritance hierarchies
- The function includes debug support via  compilation flag
- [Path](../P/Path.md) generation is skipped for certain relation types (subqueries, CTEs, etc.) that are fully handled during the size estimation phase