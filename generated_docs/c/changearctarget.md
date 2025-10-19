# changearctarget

## Location
[src/backend/regex/regc_nfa.c:533-574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L533-L574)

## Overview
Updates an arc to point to a different target state in a regular expression NFA, maintaining the integrity of the state's incoming arc chains.

## Definition
```c
static void changearctarget(struct arc *a, struct state *newto)
```

## Detailed Description
The `changearctarget` function modifies an existing arc in a Non-deterministic Finite Automaton (NFA) to point to a new target state. This operation involves carefully updating the doubly-linked list of incoming arcs for both the old and new target states. The function removes the arc from the old target state's incoming arc chain (`ins` list) and adds it to the new target state's incoming arc chain, while maintaining proper forward and reverse chain pointers.

The function assumes the caller has verified that no duplicate arc will be created by this operation. It performs several assertions to ensure data structure integrity throughout the operation.

## Parameters / Member Variables
- `a`: The arc whose target state is to be changed
- `newto`: The new target state that the arc should point to

## Dependencies
- Functions called/Symbols referenced:
  - struct arc (data structure)
  - struct state (data structure)
- Called from (representative examples):
  - [moveins](../m/moveins.md) (in regc_nfa.c:842, 865)

## Notes and Other Information
- This is a static function internal to the regex NFA construction module
- The function maintains reference counts (`nins`) for incoming arcs on both old and new target states
- Multiple assertions ensure the integrity of the doubly-linked chain structure
- The caller must ensure no duplicate arcs are created by this operation
- Part of PostgreSQL's internal regular expression engine implementation

## Simplified Source

```c
static void changearctarget(struct arc *a, struct state *newto) {
    struct state *oldto = a->to;
    struct arc *predecessor;

    // Remove arc from old target's incoming chain
    predecessor = a->inchainRev;
    if (predecessor == NULL) {
        oldto->ins = a->inchain;
    } else {
        predecessor->inchain = a->inchain;
    }
    if (a->inchain != NULL) {
        a->inchain->inchainRev = predecessor;
    }
    oldto->nins--;

    // Update arc's target pointer
    a->to = newto;

    // Add arc to new target's incoming chain
    a->inchain = newto->ins;
    a->inchainRev = NULL;
    if (newto->ins) {
        newto->ins->inchainRev = a;
    }
    newto->ins = a;
    newto->nins++;
}
```