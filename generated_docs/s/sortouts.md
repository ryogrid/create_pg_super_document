# sortouts

## Location
[src/backend/regex/regc_nfa.c:687-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L687-L728)

## Overview
Sorts the outgoing arcs of a state in PostgreSQL's regex NFA by destination state, color, and type to maintain consistent ordering.

## Definition

```c
static void
sortouts(struct nfa *nfa,
		 struct state *s)
```
## Detailed Description
This function sorts all outgoing arcs from a given NFA state using a temporary array and qsort(). The sorting maintains a consistent ordering of arcs which is important for NFA operations and optimizations. The function:

1. Creates a temporary array of arc pointers
2. Populates the array with all outgoing arcs from the state
3. Sorts the array using qsort() with sortouts_cmp as the comparison function
4. Rebuilds the doubly-linked outchain list in the sorted order
5. Cleans up the temporary array

The function handles the special case where there are 1 or fewer arcs (no sorting needed) and optimizes the rebuilding of the chain by special-casing the first and last items.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure (though not directly used in this function)
- `*s`: Pointer to the state whose outgoing arcs should be sorted
## Dependencies
- Functions called/Symbols referenced:
  - MALLOC (memory allocation macro)
  - NERR (error reporting macro)
  - REG_ESPACE (error code for out of space)
  - qsort (standard library sort function)
  - [sortouts_cmp](sortouts_cmp.md) (comparison function)
  - FREE (memory deallocation macro)
  - [arc](../a/arc.md) (struct type)
- Called from (representative examples):
  - [moveouts](../m/moveouts.md) (src/backend/regex/regc_nfa.c:1110, 1111)
  - [copyouts](../c/copyouts.md) (src/backend/regex/regc_nfa.c:1207, 1208)

## Notes and Other Information
- This is a static function local to the regc_nfa.c file
- The function maintains both forward (outchain) and reverse (outchainRev) pointers in the doubly-linked list
- Memory allocation failure is handled gracefully with error reporting
- The sorting ensures deterministic behavior in NFA operations and can improve performance of certain algorithms
- Special-casing first and last items in the chain rebuilding reduces conditional checks in the main loop

## Simplified Source

```c
static void sortouts(struct nfa *nfa, struct state *s) {
    struct arc **sortarray;
    struct arc *a;
    int n = s->nouts;
    int i;

    // Skip sorting if 0 or 1 arcs
    if (n <= 1)
        return;

    // Create array of arc pointers
    sortarray = (struct arc **) MALLOC(n * sizeof(struct arc *));
    if (sortarray == NULL) {
        NERR(REG_ESPACE);
        return;
    }

    // Populate array from outgoing arc chain
    i = 0;
    for (a = s->outs; a != NULL; a = a->outchain)
        sortarray[i++] = a;

    // Sort the array by destination state, color, and type
    qsort(sortarray, n, sizeof(struct arc *), sortouts_cmp);

    // Rebuild the outgoing chain in sorted order
    s->outs = sortarray[0];
    sortarray[0]->outchain = (n > 1) ? sortarray[1] : NULL;
    sortarray[0]->outchainRev = NULL;

    for (i = 1; i < n - 1; i++) {
        sortarray[i]->outchain = sortarray[i + 1];
        sortarray[i]->outchainRev = sortarray[i - 1];
    }

    if (n > 1) {
        sortarray[n-1]->outchain = NULL;
        sortarray[n-1]->outchainRev = sortarray[n - 2];
    }

    FREE(sortarray);
}
```