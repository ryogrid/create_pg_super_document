# merge_clump

## Location
[src/backend/optimizer/geqo/geqo_eval.c:238-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_eval.c#L238-L324)

## Overview
Merges a single clump (group of joined relations) into an existing list of clumps, attempting joins with existing clumps and maintaining size-ordered list structure.

## Definition

```c
struct a RelOptInfo representing the join of these two input
			 * relations.  Note that we expect the joinrel not to exist in
			 * root->join_rel_list yet, and so the paths constructed for it
			 * will only include the ones we want.
			 */
			joinrel = make_join_rel(root,
									old_clump->joinrel,
									new_clump->joinrel);
```
## Detailed Description
The merge_clump function is a core component of gimme_tree's clump-based join construction algorithm. It attempts to merge a new clump with existing clumps in the list, repeating the process recursively when successful merges occur. The function serves as the primary mechanism for building larger join relations from smaller ones while respecting join constraints and heuristics.

The algorithm iterates through existing clumps, checking if the new clump can be joined with each one. Join decisions are based on either heuristic desirability (when force is false) or simple legality (when force is true). When a valid join is found, it constructs the actual join relation using make_join_rel(), generates appropriate paths including partitionwise joins and gather paths, and then recursively attempts to merge the enlarged clump with others.

If no merge is possible, the new clump is inserted into the list in size-descending order, with larger clumps appearing earlier. This ordering helps optimize the joining process by prioritizing larger intermediate results.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and join-related metadata
- : List of existing Clump structures, maintained in size-descending order
- : The single Clump to be merged into the existing list
- : Total number of relations being joined (used for recursion context)
- : Boolean flag determining join strategy - true for any legal join, false for desirable joins only

## Dependencies
- Functions called/Symbols referenced:
  - [desirable_join](../d/desirable_join.md) (evaluates join desirability using heuristics)
  - [make_join_rel](make_join_rel.md) (constructs actual join relations)
  - [generate_partitionwise_join_paths](../g/generate_partitionwise_join_paths.md) (creates partitioned table join paths)
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md) (creates parallel execution paths)
  - [set_cheapest](../s/set_cheapest.md) (finds optimal paths for the join relation)
  - [bms_equal](../b/bms_equal.md) (bitmap set comparison)
  - foreach_delete_current, list_nth, list_insert_nth (list manipulation)
  - Clump (data structure type)

- Called from (representative examples):
  - [gimme_tree](../g/gimme_tree.md) (main tree construction algorithm)
  - [merge_clump](merge_clump.md) (recursive self-calls for continued merging)

## Notes and Other Information
- Implements recursive merging - successful joins trigger attempts to merge the enlarged clump further
- Maintains clumps list in size-descending order for optimization
- Handles two modes: heuristic-based desirable joins vs. force-joining any legal combination
- Generates comprehensive path sets including partitionwise and parallel paths for each join
- Critical for building bushy join trees that respect both semantic constraints and optimization heuristics
- Uses immediate cleanup of merged clumps to prevent memory leaks