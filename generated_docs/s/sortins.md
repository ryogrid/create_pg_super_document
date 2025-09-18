# sortins

## Location
[src/backend/regex/regc_nfa.c:620-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L620-L661)

## Overview
Sorts the incoming arcs of a state in an NFA by their source state, color, and type to maintain a canonical ordering.

## Definition
```c
static void sortins(struct nfa *nfa, struct state *s)
```

## Detailed Description
The `sortins` function reorders the incoming arcs (`ins` chain) of a state to maintain a consistent, sorted arrangement. This sorting is important for NFA operations that rely on having arcs in a predictable order, particularly during state merging and optimization phases.

The function creates a temporary array of arc pointers, populates it with all incoming arcs from the state's `ins` chain, sorts the array using `qsort` with a comparison function `sortins_cmp`, and then rebuilds the doubly-linked incoming arc chain in the sorted order. The sorting is done by source state, color, and type properties of the arcs.

For performance reasons, the function includes an early exit for states with one or fewer incoming arcs, as sorting is unnecessary in these cases. The function also handles memory allocation errors gracefully by setting the regex error state and returning early.

## Parameters / Member Variables
- `nfa`: The NFA structure containing the state (used for error reporting context)
- `s`: The state whose incoming arcs should be sorted

## Dependencies
- Functions called/Symbols referenced:
  - struct arc (data structure)
  - struct state (data structure)
  - struct nfa (data structure)
  - MALLOC (memory allocation macro)
  - NERR (error reporting macro)
  - REG_ESPACE (out of memory error constant)
  - qsort (standard library sorting function)
  - [sortins_cmp](sortins_cmp.md) (comparison function for arc sorting)
  - FREE (memory deallocation macro)
- Called from (representative examples):
  - [moveins](../m/moveins.md) (in regc_nfa.c:822, 823)
  - [copyins](../c/copyins.md) (in regc_nfa.c:922, 923)
  - [mergeins](../m/mergeins.md) (in regc_nfa.c:990)

## Notes and Other Information
- This is a static function internal to the regex NFA construction module
- Optimizes for the common case by skipping states with 0 or 1 incoming arcs
- Uses standard library `qsort` with a custom comparison function `sortins_cmp`
- Carefully rebuilds the doubly-linked chain structure after sorting
- Special-cases the first and last items in the rebuild loop for efficiency
- Handles memory allocation failures by setting regex error state
- Part of PostgreSQL's internal regular expression engine implementation
- Essential for maintaining canonical state representations during NFA transformations