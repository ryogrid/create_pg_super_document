# BipartiteMatchState

## Location
[src/include/lib/bipartite_match.h:27-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/bipartite_match.h#L27-L40)

## Overview
BipartiteMatchState is a data structure that represents the state of a bipartite graph matching algorithm. It stores input parameters, output results, and private algorithm state for finding maximum cardinality matchings in bipartite graphs.

## Definition


## Detailed Description
BipartiteMatchState encapsulates the complete state needed for computing maximum cardinality matchings in bipartite graphs using the Hopcroft-Karp algorithm. A bipartite graph consists of two disjoint sets of nodes U (numbered 1..nU) and V (numbered 1..nV), with edges only between nodes from different sets.

The structure is designed to find the maximum number of edges such that no node appears in more than one edge. This has practical applications in PostgreSQL for optimizing grouping sets by applying Dilworth's theorem, which helps minimize the number of sort operations needed for a collection of grouping sets.

The algorithm uses the Hopcroft-Karp approach which alternates between breadth-first search to find augmenting paths and depth-first search to find vertex-disjoint augmenting paths, achieving O(E√V) time complexity.

## Parameters / Member Variables
- : Size of the U node set (nodes numbered 1 to u_size)
- : Size of the V node set (nodes numbered 1 to v_size)
- : 2D array where adjacency[u] = [k, v1, v2, v3, ..., vk], with k being the count of adjacent V nodes
- : Output field containing the number of edges in the computed maximum matching
- : Output array mapping each U node to its matched V node (0 if unmatched)
- : Output array mapping each V node to its matched U node (0 if unmatched)
- : Private array storing distance values for U nodes during breadth-first search
- : Private array used as queue storage during breadth-first search phases

## Dependencies
- Functions called/Symbols referenced:
  - HK_INFINITY (constant used in algorithm)
- Called from (representative examples):
  - [BipartiteMatch](BipartiteMatch.md) (constructor function)
  - [BipartiteMatchFree](BipartiteMatchFree.md) (destructor function)
  - [hk_breadth_search](../h/hk_breadth_search.md) (internal algorithm function)
  - [hk_depth_search](../h/hk_depth_search.md) (internal algorithm function)
  - [extract_rollup_sets](../e/extract_rollup_sets.md) (in query planner)

## Notes and Other Information
- The structure is allocated and initialized by BipartiteMatch() and should be freed using BipartiteMatchFree()
- [Node](../N/Node.md) indices are 1-based rather than 0-based throughout the implementation
- The adjacency list is owned by the caller and not freed by BipartiteMatchFree()
- Size limits are enforced (must be less than SHRT_MAX) due to the use of short integers
- The algorithm implementation includes interrupt checks for long-running computations
- Primary use case in PostgreSQL is for optimizing grouping set operations via Dilworth's theorem
- The matching results can be read from the pair_uv and pair_vu arrays after BipartiteMatch() completes