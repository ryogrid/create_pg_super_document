# join_search_one_level

## Location
src/backend/optimizer/path/joinrels.c: 73 - 279

## Overview
A core function in PostgreSQL's dynamic programming join search algorithm that considers all ways to produce join relations containing exactly 'level' jointree items in one step of the optimization process.

## Definition


## Detailed Description
The  function is a fundamental component of PostgreSQL's standard join search algorithm. It implements one step of the dynamic programming method used to find optimal join orders. The function systematically generates all feasible join combinations at a specific level by:

1. **Left-sided and right-sided plans**: Joining relations of exactly (level-1) members against initial relations, preferring joins with join clauses over Cartesian products
2. **Bushy plans**: Considering joins between relations of k initial rels with relations of (level-k) initial rels for intermediate values of k
3. **Fallback Cartesian products**: As a last resort, generating Cartesian product joins when no clause-based joins are possible

The function uses join clauses, equivalence classes, and join order restrictions to guide the search and avoid generating unreasonable numbers of join combinations.

## Parameters / Member Variables
- : PlannerInfo structure containing the query planning context and join relation levels
- : The target level (number of relations) for joins to be created in this iteration

## Dependencies
- Functions called/Symbols referenced:
  - has_join_restriction
  - foreach_current_index
  - make_rels_by_clause_joins
  - make_rels_by_clauseless_joins
  - for_each_from
  - bms_overlap
  - have_relevant_joinclause
  - have_join_order_restriction
  - make_join_rel
- Called from (representative examples):
  - standard_join_search

## Notes and Other Information
- The function modifies  to ensure new joinrels are added to the proper list
- Results are stored in 
- Special handling exists for sub-joinlist scenarios where all relations have only external join clauses
- Includes sanity checking to detect cases where no legal joins can be formed when no special joins or lateral references exist
- The algorithm avoids duplicate work by leveraging symmetry in join operations and careful iteration bounds