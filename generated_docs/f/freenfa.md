# freenfa

## Location
[src/backend/regex/regc_nfa.c:107-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L107-L136)

## Overview
Deallocates and frees an entire NFA structure and all its associated memory, including state batches and arc batches.

## Definition

```c
static void
freenfa(struct nfa *nfa)
```
## Detailed Description
The  function performs complete cleanup of an NFA structure by freeing all allocated memory. It iterates through linked lists of state batches and arc batches, deallocating each batch and updating the space usage counter in the vars structure. The function ensures proper memory management by traversing and freeing all dynamically allocated components of the NFA, making it safe to call even on partially initialized NFAs.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure to be freed
## Dependencies
- Functions called/Symbols referenced:
  - STATEBATCHSIZE
  - FREE
  - ARCBATCHSIZE
- Called from (representative examples):
  - [newnfa](../n/newnfa.md) (in regc_nfa.c)
  - [freev](freev.md) (in regcomp.c)
  - [nfanode](../n/nfanode.md) (in regcomp.c)

## Notes and Other Information
- Safely handles NULL pointers and partially initialized NFAs
- Updates space usage counters in vars structure during deallocation
- Sets nstates to -1 before freeing the main structure as a safety measure
- Critical for preventing memory leaks in regex compilation error paths
- Must be called for any NFA created by newnfa to avoid memory leaks

## Simplified Source

```c
static void freenfa(struct nfa *nfa) {
    struct statebatch *sb, *sbnext;
    struct arcbatch *ab, *abnext;

    // Free all state batches in the linked list
    for (sb = nfa->lastsb; sb != NULL; sb = sbnext) {
        sbnext = sb->next;
        nfa->v->spaceused -= STATEBATCHSIZE(sb->nstates);  // Update memory usage
        FREE(sb);
    }
    nfa->lastsb = NULL;

    // Free all arc batches in the linked list
    for (ab = nfa->lastab; ab != NULL; ab = abnext) {
        abnext = ab->next;
        nfa->v->spaceused -= ARCBATCHSIZE(ab->narcs);      // Update memory usage
        FREE(ab);
    }
    nfa->lastab = NULL;

    // Mark as invalid and free the main NFA structure
    nfa->nstates = -1;
    FREE(nfa);
}
```