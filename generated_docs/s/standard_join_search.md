# standard_join_search

## Location
[src/backend/optimizer/path/allpaths.c:3411-3581](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3411-L3581)

## Overview
Implements PostgreSQL's standard dynamic programming algorithm to find optimal join paths by systematically building join relations from component relations through successive levels.

## Definition
RelOptInfo *standard_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)

## Detailed Description
This function is the heart of PostgreSQL's standard join optimization strategy, implementing a classic dynamic programming approach to find the optimal way to join multiple relations. It operates by building join relations level by level:

1. **Level 1**: Contains all single-item relations (base tables or sub-joinlists)
2. **Level 2**: All possible 2-way joins from level 1 relations  
3. **Level 3**: All possible 3-way joins (2-way joins + single relations)
4. **Continue** until all relations are joined into a single final relation

The algorithm ensures that all possible join orders are considered while avoiding redundant computation through memoization in the join_rel_level array. After creating paths at each level, it generates partitionwise join paths, gather paths for parallelism, and determines the cheapest paths.

The function is designed to be replaceable by plugins via join search hooks, enabling custom join optimization strategies while maintaining compatibility with the standard optimizer framework.

## Parameters / Member Variables
- : PlannerInfo containing global planning context and join relation storage
- : Number of iterations required (equals number of independent jointree items)
- : List of RelOptInfo nodes for base relations to be joined together

## Dependencies
- Functions called/Symbols referenced:
  - [join_search_one_level](../j/join_search_one_level.md): Builds all possible joins at a specific level
  - [generate_partitionwise_join_paths](../g/generate_partitionwise_join_paths.md): Creates partition-aware join paths
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md): Creates parallel execution paths with Gather nodes
  - [set_cheapest](set_cheapest.md): Identifies and saves the lowest-cost paths
  - [bms_equal](../b/bms_equal.md): Bitmap set equality comparison
  - [pprint](../p/pprint.md): Debug output function (when OPTIMIZER_DEBUG enabled)
- Called from (representative examples):
  - [make_rel_from_joinlist](../m/make_rel_from_joinlist.md): When standard join search is selected over GEQO

## Notes and Other Information
- Cannot be invoked recursively within a single planning problem
- Uses root->join_rel_level array for dynamic programming memoization
- Supports plugin architecture through join search hooks
- Generates partitionwise joins for partition-aware query optimization  
- Creates gather paths for parallel query execution (except for topmost relation)
- Plugin authors must preserve original join_rel_list and join_rel_hash states
- Always produces exactly one final relation containing all joined tables
- Critical performance component - complexity grows exponentially with join count