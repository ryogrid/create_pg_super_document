# hk_depth_search

## Location
src/backend/lib/bipartite_match.c: 146 - 180

## Overview
hk_depth_search implements the depth-first search phase of the Hopcroft-Karp algorithm, finding and updating augmenting paths to improve the current bipartite matching.

## Definition
```c
static bool hk_depth_search(BipartiteMatchState *state, int u)
```

## Detailed Description
This function performs the DFS phase of the Hopcroft-Karp maximum bipartite matching algorithm. Starting from an unmatched vertex u in set U, it recursively explores the level structure created by hk_breadth_search to find an augmenting path. The function follows alternating paths: unmatched edges from U to V, then matched edges from V back to U, continuing until it reaches an unmatched vertex in V (represented by pair_vu[v] == 0). When an augmenting path is found, the function backtracks and updates the matching arrays to flip the matched/unmatched status of edges along the path. The recursive nature allows multiple vertex-disjoint augmenting paths to be found simultaneously. If no augmenting path exists from the current vertex, it marks the vertex as unreachable (distance = HK_INFINITY) to prevent redundant exploration.

## Parameters / Member Variables
- `state`: Pointer to BipartiteMatchState containing the bipartite graph and matching information
- `u`: Starting vertex in set U from which to search for an augmenting path (0 represents NIL/unmatched)

## Dependencies
- Functions called/Symbols referenced:
  - BipartiteMatchState (struct type for algorithm state)
  - HK_INFINITY (constant representing unreachable/infinite distance)
  - check_stack_depth (PostgreSQL function to prevent stack overflow in deep recursion)
  - hk_depth_search (recursive self-call to explore deeper levels)
- Called from (representative examples):
  - BipartiteMatch (main algorithm calls DFS for each unmatched vertex in U)
  - hk_depth_search (recursive calls during path exploration)

## Notes and Other Information
- Uses recursive DFS with memoization (distance array) to avoid redundant path exploration
- The base case (u == 0) represents reaching an unmatched vertex in V, indicating a successful augmenting path
- Updates matching arrays (pair_uv, pair_vu) when augmenting paths are found, effectively flipping edge status
- Includes stack depth checking to prevent stack overflow in deep recursion scenarios
- Sets distance[u] to HK_INFINITY when no augmenting path exists from u, serving as memoization for efficiency
- Critical component enabling the O(E√V) complexity of Hopcroft-Karp by finding multiple disjoint augmenting paths per iteration
- The algorithm only explores edges that maintain the level structure established by the BFS phase