# gimme_tree

## Location
[src/backend/optimizer/geqo/geqo_eval.c:163-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_eval.c#L163-L237)

## Overview
Constructs a join tree from a given gene tour in the GEQO algorithm, using heuristics to build valid bushy plans while respecting join order constraints and semantic restrictions.

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
The gimme_tree function is responsible for converting a gene tour (join order sequence) into an actual join tree that can be evaluated for cost. Unlike earlier implementations that could only build left-sided plans, this version uses sophisticated heuristics to generate bushy plans when necessary.

The algorithm maintains a list of "clumps" - groups of successfully joined relations, with larger clumps positioned at the front. Each relation from the tour is processed sequentially and added to the first clump it can legally join with. If no suitable clump exists, it forms a new single-relation clump. When a clump is enlarged, the algorithm checks if it can be merged with other clumps.

The process occurs in two phases: first, relations are joined using only desirable joins according to heuristics. If multiple clumps remain after processing the entire tour, a second phase force-joins the remaining clumps in any legal order. The function succeeds only if all relations can be merged into a single clump, representing a complete join tree.

## Parameters / Member Variables
- : PlannerInfo structure containing the query planning context and metadata
- : Array of Gene values representing the proposed join order to be converted into a plan
- : Integer specifying the number of genes (relations) in the tour array

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md) (accesses relations from initial_rels list)
  - [merge_clump](../m/merge_clump.md) (merges clumps using join heuristics)
  - [palloc](../p/palloc.md) (memory allocation for clump structures)
  - GeqoPrivateData, Clump, Gene (data structure types)

- Called from (representative examples):
  - [geqo_eval](geqo_eval.md) (fitness evaluation in genetic algorithm)
  - [geqo](geqo.md) (main genetic optimization routine)

## Notes and Other Information
- Can generate bushy plans, unlike earlier left-sided-only implementations
- Uses heuristic rules to determine suitable joins, which may occasionally fail
- Returns NULL if the gene tour cannot be converted into a valid complete join tree
- Handles join order restrictions and semantic constraints through the clumping mechanism
- May fail with LATERAL restrictions if relations are clumped inappropriately without ability to un-clump
- Critical component bridging genetic algorithm gene representation and actual query plan construction