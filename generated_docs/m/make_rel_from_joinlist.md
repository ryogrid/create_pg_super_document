# make_rel_from_joinlist

## Location
[src/backend/optimizer/path/allpaths.c:3306-3410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3306-L3410)

## Overview
Builds access paths using a "joinlist" to guide the join path search, serving as the core recursive function for constructing optimal join relations from a structured joinlist.

## Definition
static RelOptInfo *make_rel_from_joinlist(PlannerInfo *root, List *joinlist)

## Detailed Description
This function is a central component of PostgreSQL's query optimizer that takes a structured joinlist (created by deconstruct_jointree()) and builds optimal RelOptInfo structures representing join relations. It employs a dynamic programming approach to consider all possible ways of joining child nodes.

The function handles two main cases:
1. **Single relation case**: When the joinlist contains only one element, it returns that relation directly
2. **Multiple relations case**: When multiple relations need to be joined, it delegates to appropriate join search algorithms (plugin hook, GEQO, or standard join search)

The function recursively processes nested joinlists, converting RangeTblRef nodes to base relations and List nodes to sub-join problems. It builds an initial_rels list containing all relations that need to be joined at the current level.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimization context and state
- : List of join nodes that can contain RangeTblRef (base tables) or nested Lists (sub-joinlists)

## Dependencies
- Functions called/Symbols referenced:
  - [find_base_rel](../f/find_base_rel.md): Retrieves base relation information
  - [make_rel_from_joinlist](make_rel_from_joinlist.md): Recursive calls for sub-joinlists  
  - [geqo](../g/geqo.md): Genetic Query Optimizer for complex joins
  - [standard_join_search](../s/standard_join_search.md): Standard dynamic programming join search
  - nodeTag: Node type identification
- Called from (representative examples):
  - [make_one_rel](make_one_rel.md): Main entry point for relation optimization
  - [make_rel_from_joinlist](make_rel_from_joinlist.md): Recursive self-calls

## Notes and Other Information
- Uses dynamic programming with depth determined by joinlist length
- Supports pluggable join search algorithms via join_search_hook
- Automatically switches to GEQO when join complexity exceeds geqo_threshold
- The initial_rels list is stored in PlannerInfo for use by has_legal_joinclause()
- Handles error cases for unrecognized joinlist node types
- Critical function in PostgreSQL's cost-based optimizer architecture