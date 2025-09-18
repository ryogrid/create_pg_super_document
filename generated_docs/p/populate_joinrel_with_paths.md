# populate_joinrel_with_paths

## Location
[src/backend/optimizer/path/joinrels.c:894-1071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L894-L1071)

## Overview
Adds execution paths to a join relation by considering all possible join types and optimizations, handling dummy relations and constant-false restrictions.

## Definition


## Detailed Description
The  function is the core path generation engine for join relations in PostgreSQL's query optimizer. It systematically considers different join execution strategies based on the join type (INNER, LEFT, FULL, SEMI, ANTI) and creates corresponding execution paths. The function implements sophisticated logic to handle edge cases such as provably empty relations (dummy rels) and constant-false join restrictions that can eliminate entire join branches.

For each join type, the function evaluates whether the join can produce any results and marks relations as dummy when appropriate. It considers both directions of joining (rel1⋈rel2 and rel2⋈rel1) and applies join-specific optimizations. For SEMI joins, it can transform them into regular joins with uniqueness constraints when beneficial. The function also handles partitionwise joining as a final optimization step.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning context
- : First RelOptInfo representing one input relation to the join
- : Second RelOptInfo representing the other input relation to the join  
- : The target RelOptInfo that will receive the generated join paths
- : SpecialJoinInfo containing join type and constraint information
- : List of join clauses and other applicable restrictions for this join pair

## Dependencies
- Functions called/Symbols referenced:
  - [is_dummy_rel](../i/is_dummy_rel.md)
  - [restriction_is_constant_false](../r/restriction_is_constant_false.md)
  - [mark_dummy_rel](../m/mark_dummy_rel.md)
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [create_unique_path](../c/create_unique_path.md)
  - [try_partitionwise_join](../t/try_partitionwise_join.md)
- Called from (representative examples):
  - [make_join_rel](../m/make_join_rel.md)
  - [try_partitionwise_join](../t/try_partitionwise_join.md)

## Notes and Other Information
- Function is static to the joinrels.c file and not exposed externally
- Implements different logic paths for each join type: INNER, LEFT, FULL, SEMI, ANTI
- For FULL joins, validates that merge-joinable or hash-joinable conditions exist, erroring otherwise
- SEMI joins can be converted to UNIQUE_INNER/UNIQUE_OUTER joins when the RHS can be uniquified
- Handles constant-false restrictions differently for pushed-down vs non-pushed-down cases in outer joins
- Always attempts partitionwise joining as a final optimization opportunity
- Critical for ensuring all viable execution paths are considered while avoiding impossible joins