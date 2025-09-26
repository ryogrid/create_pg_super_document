# build_join_rel

## Location
[src/backend/optimizer/util/relnode.c:665-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L665-L880)

## Overview
Creates and initializes a RelOptInfo structure representing the join of two relations, handling both the creation of new join relations and retrieval of existing ones.

## Definition

```c
union(outer_rel->direct_lateral_relids,
				  inner_rel->direct_lateral_relids);
```
## Detailed Description
This function is the central hub for creating join relations in PostgreSQL's query optimizer. It first checks if a join relation for the given set of base relations already exists using . If found, it only needs to compute the restrictlist for the specific pair of relations. If not found, it creates a new  node of type  and initializes all its fields.

The function performs several critical tasks:
1. Creates and initializes a new join relation structure
2. Computes lateral parameterization using 
3. Builds the target list by calling  for both outer and inner relations
4. Constructs restrict and join clause lists
5. Sets size estimates for the join relation
6. Determines parallel execution feasibility
7. Adds the join relation to the planner's data structures

## Parameters / Member Variables
- : PlannerInfo containing global planner state and context
- : Relids set uniquely identifying the join relation
- : RelOptInfo for the outer relation to be joined
- : RelOptInfo for the inner relation to be joined  
- : SpecialJoinInfo containing join context and constraints
- : List of any pushed-down outer joins that are now completed
- : Output parameter receiving the list of RestrictInfo nodes for this join pair

## Dependencies
- Functions called/Symbols referenced:
  - find_join_rel
  - build_joinrel_restrictlist
  - min_join_parameterization
  - build_joinrel_tlist
  - add_placeholders_to_joinrel
  - build_joinrel_joinlist
  - has_relevant_eclass_joinclause
  - build_joinrel_partition_info
  - set_joinrel_size_estimates
  - set_foreign_rel_properties
  - add_join_rel
- Called from (representative examples):
  - make_join_rel

## Notes and Other Information
- This function should only be used for joins between parent relations (not other rel types)
- The target list order for a join relation depends on which pair of outer/inner relations is first used to build it, but the contents remain consistent
- The function handles parallel execution considerations by checking if both input relations support parallel execution and if all expressions are parallel-safe
- The restrictlist_ptr parameter makes the API somewhat awkward but avoids duplicated restrictlist calculations
- For dynamic-programming join search, the new join relation is added to the appropriate level sublist