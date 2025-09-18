# BipartiteMatch

## Location
src/backend/lib/bipartite_match.c: 39 - 77

## Overview
BipartiteMatch implements the Hopcroft-Karp algorithm to find maximum matching in a bipartite graph, returning a state object containing the matching results and intermediate data structures.

## Definition


## Detailed Description
This function performs maximum bipartite matching using the Hopcroft-Karp algorithm. It takes two sets U and V of specified sizes and an adjacency list representation of the bipartite graph. The algorithm works by repeatedly finding augmenting paths using breadth-first search (BFS) to build level structures and depth-first search (DFS) to find actual augmenting paths. The function allocates and initializes a BipartiteMatchState structure to track the matching process and returns it with the final matching results.

The algorithm validates input sizes to ensure they don't exceed SHRT_MAX limits, then iteratively improves the matching by finding augmenting paths until no more can be found, resulting in a maximum matching.

## Parameters / Member Variables
- : Size of set U (must be >= 0 and < SHRT_MAX), vertices indexed 1..u_size
- : Size of set V (must be >= 0 and < SHRT_MAX), vertices indexed 1..v_size  
- : Adjacency list representation where adjacency[u] contains vertices in V adjacent to vertex u in U

## Dependencies
- Functions called/Symbols referenced:
  - [BipartiteMatchState](BipartiteMatchState.md) (struct type)
  - [hk_breadth_search](../h/hk_breadth_search.md) (builds level structure for augmenting paths)
  - [hk_depth_search](../h/hk_depth_search.md) (finds actual augmenting paths using DFS)
  - [palloc](../p/palloc.md), palloc0 (PostgreSQL memory allocation)
  - elog (PostgreSQL error logging)
  - CHECK_FOR_INTERRUPTS (PostgreSQL interrupt handling)
- Called from (representative examples):
  - [extract_rollup_sets](../e/extract_rollup_sets.md) (in query planning for ROLLUP operations)

## Notes and Other Information
- Implements the Hopcroft-Karp algorithm which achieves O(E√V) time complexity for maximum bipartite matching
- Input validation ensures sizes are within short integer bounds to prevent overflow
- Uses 1-based indexing for vertices as is common in graph algorithm literature
- The returned state contains the complete matching information and can be used with BipartiteMatchFree for cleanup
- Includes interrupt checking to allow PostgreSQL to handle query cancellation during long-running matching operations
- Memory is allocated using PostgreSQL's memory management system (palloc family)