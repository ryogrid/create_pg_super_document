# make_join_rel

## Location
src/backend/optimizer/path/joinrels.c: 705 - 801

## Overview
Creates or finds a join RelOptInfo representing the join of two relations, adding path information for joins with the two relations as outer and inner.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's query optimizer that handles the creation of join relations. It takes two relations and attempts to create a valid join between them, considering various join types including outer joins, and constraints from IN or EXISTS clauses that have been converted to joins. The function performs validity checks, determines the appropriate join type, constructs the canonical relation identifiers including any outer join dependencies, and populates the resulting join relation with possible execution paths.

The function can return NULL if the attempted join is not valid, which commonly occurs when working with outer joins or with complex subquery constructs. It ensures that no overlapping relation sets are joined and handles the complexity of outer join ordering requirements.

## Parameters / Member Variables
- : The PlannerInfo structure containing global information about the query being planned
- : The first RelOptInfo representing one of the relations to be joined  
- : The second RelOptInfo representing the other relation to be joined

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_union](../b/bms_union.md)
  - [join_is_legal](../j/join_is_legal.md)
  - [add_outer_joins_to_relids](../a/add_outer_joins_to_relids.md)
  - [init_dummy_sjinfo](../i/init_dummy_sjinfo.md)
  - build_join_rel
  - [is_dummy_rel](../i/is_dummy_rel.md)
  - [populate_joinrel_with_paths](../p/populate_joinrel_with_paths.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [join_search_one_level](../j/join_search_one_level.md)
  - [make_rels_by_clause_joins](make_rels_by_clause_joins.md)
  - [make_rels_by_clauseless_joins](make_rels_by_clauseless_joins.md)
  - [merge_clump](merge_clump.md)

## Notes and Other Information
- The function includes an assertion to verify that the two input relations do not have overlapping base relation sets
- Returns NULL for invalid joins, particularly important for outer join constraints
- Handles relation swapping when needed to match join information requirements
- Creates dummy SpecialJoinInfo for plain inner joins when no specific join information exists
- The resulting join relation may already contain paths from other relation pairs that form the same base relation set
- Memory management is handled through bms_free calls for temporary bitmap sets