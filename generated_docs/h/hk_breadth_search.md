# hk_breadth_search

## Location
[src/backend/lib/bipartite_match.c:93-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/bipartite_match.c#L93-L145)

## Overview
hk_breadth_search implements the breadth-first search phase of the Hopcroft-Karp algorithm, building level structures to identify potential augmenting paths in the bipartite matching.

## Definition
```c
static bool hk_breadth_search(BipartiteMatchState *state)
```

## Detailed Description
This function performs the BFS phase of the Hopcroft-Karp maximum bipartite matching algorithm. It constructs a level graph by assigning distance values to vertices in set U, starting from unmatched vertices (distance 0) and propagating through the graph via alternating paths. The algorithm uses a queue to explore vertices level by level, following unmatched edges from U to V and matched edges from V back to U. The BFS terminates when it reaches unmatched vertices in V or when no more progress can be made. The function returns true if augmenting paths exist (indicated by distance[0] being finite), enabling the subsequent depth-first search phase to find actual augmenting paths.

The level structure created by this BFS ensures that the DFS phase can find multiple vertex-disjoint augmenting paths simultaneously, which is key to the Hopcroft-Karp algorithm's efficiency.

## Parameters / Member Variables
- `state`: Pointer to BipartiteMatchState containing the bipartite graph and current matching state

## Dependencies
- Functions called/Symbols referenced:
  - [BipartiteMatchState](../B/BipartiteMatchState.md) (struct type for algorithm state)
  - HK_INFINITY (constant representing infinite distance for unreachable vertices)
  - Assert (PostgreSQL assertion macro for debugging)
- Called from (representative examples):
  - [BipartiteMatch](../B/BipartiteMatch.md) (main algorithm driver that calls BFS iteratively)

## Notes and Other Information
- Uses distance[0] as a sentinel to track the minimum distance to any unmatched vertex in V
- Implements level-based BFS where distances represent the length of alternating paths from unmatched U vertices
- The queue implementation is optimized for single-use (no wraparound needed since vertices are enqueued at most once per iteration)
- Returns true when augmenting paths exist, false when maximum matching is achieved
- Critical for the O(E√V) time complexity of Hopcroft-Karp by enabling multiple augmenting paths to be found in each iteration
- The adjacency list format stores the count at index 0 followed by the actual adjacent vertices

## Simplified Source

```c
static bool hk_breadth_search(BipartiteMatchState *state) {
    int usize = state->u_size;
    short *queue = state->queue;
    short *distance = state->distance;
    int qhead = 0, qtail = 0;

    // Initialize distances - unmatched vertices get distance 0
    distance[0] = HK_INFINITY;
    for (int u = 1; u <= usize; u++) {
        if (state->pair_uv[u] == 0) {
            distance[u] = 0;
            queue[qhead++] = u;
        } else {
            distance[u] = HK_INFINITY;
        }
    }

    // BFS to build level structure
    while (qtail < qhead) {
        int u = queue[qtail++];

        if (distance[u] < distance[0]) {
            short *u_adj = state->adjacency[u];
            int adj_count = u_adj ? u_adj[0] : 0;

            // Process all adjacent vertices
            for (int i = adj_count; i > 0; i--) {
                int u_next = state->pair_vu[u_adj[i]];

                if (distance[u_next] == HK_INFINITY) {
                    distance[u_next] = 1 + distance[u];
                    queue[qhead++] = u_next;
                }
            }
        }
    }

    return (distance[0] != HK_INFINITY);
}
```