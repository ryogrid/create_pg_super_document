# BipartiteMatchFree

## Location
src/backend/lib/bipartite_match.c: 78 - 92

## Overview
BipartiteMatchFree deallocates memory for a BipartiteMatchState structure returned by BipartiteMatch, cleaning up all internal data structures while preserving the caller-owned adjacency list.

## Definition
```c
void BipartiteMatchFree(BipartiteMatchState *state)
```

## Detailed Description
This function performs cleanup for a BipartiteMatchState object created by BipartiteMatch. It systematically deallocates all memory allocated during the matching process, including the matching arrays (pair_uv, pair_vu), working arrays (distance, queue), and the state structure itself. The function intentionally does not free the adjacency list since it is owned by the caller and may be needed for other operations. The cleanup is optional since PostgreSQL's memory contexts will automatically free the memory when the context is destroyed, but explicit cleanup can be useful for long-running operations or when memory usage needs to be tightly controlled.

## Parameters / Member Variables
- `state`: Pointer to BipartiteMatchState structure to be freed (created by BipartiteMatch)

## Dependencies
- Functions called/Symbols referenced:
  - BipartiteMatchState (struct type)
  - pfree (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - extract_rollup_sets (in query planning after ROLLUP matching is complete)

## Notes and Other Information
- Memory cleanup is optional in PostgreSQL due to memory context management, but recommended for explicit resource management
- Does not free the adjacency list as it is considered caller-owned data
- Should only be called on states returned by BipartiteMatch
- Frees memory in reverse dependency order: arrays first, then the state structure
- Part of PostgreSQL's resource management best practices for preventing memory leaks in long-running operations