# mergeins

## Location
[src/backend/regex/regc_nfa.c:971-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L971-L1065)

## Overview
Merges a list of incoming arcs into a state by eliminating duplicates and adding only unique arcs.

## Definition

```c
static void
mergeins(struct nfa *nfa,
		 struct state *s,
		 struct arc **arcarray,
		 int arccount)
```
## Detailed Description
The  function is an optimized version of  that processes an array of arc pointers rather than individual arcs. It merges incoming arcs from multiple sources into a target state while eliminating duplicates. The function first sorts both the existing incoming arcs of the target state and the provided arc array, then performs a merge operation to add only unique arcs. This approach is more efficient than processing arcs individually when dealing with multiple arc sources, as it avoids creating duplicate arcs that would need to be removed later.

The function includes interrupt checking to allow for cancellation of long-running operations and uses a sort-merge algorithm to efficiently handle large numbers of arcs.

## Parameters / Member Variables
- : Pointer to the NFA (Non-deterministic Finite Automaton) structure being modified
- : Target state that will receive the merged incoming arcs  
- : Array of arc pointers to be merged into the target state (contents may be modified)
- : Number of arcs in the arcarray

## Dependencies
- Functions called/Symbols referenced:
  - INTERRUPT
  - [sortins](../s/sortins.md)
  - NISERR
  - qsort
  - [sortins_cmp](../s/sortins_cmp.md)
  - [createarc](../c/createarc.md)
  - NOTREACHED
- Called from (representative examples):
  - [fixempties](../f/fixempties.md)

## Notes and Other Information
- The function is designed to handle non-unique source arcs and will eliminate duplicates during processing
- It's acceptable to modify the contents of arcarray during execution
- The function performs interrupt checking to allow cancellation during long operations
- Uses a sort-merge approach for efficiency when dealing with large numbers of arcs
- Located in src/backend/regex/regc_nfa.c:971-1065