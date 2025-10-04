# findconstraintloop

## Location
[src/backend/regex/regc_nfa.c:2469-2557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L2469-L2557)

## Overview
Recursively searches for loops of constraint arcs in the NFA and breaks them when found to prevent infinite loops during regex compilation.

## Definition

```c
static int findconstraintloop(struct nfa *nfa, struct state *s)
```
## Detailed Description
This function implements a depth-first search algorithm to detect constraint loops in the NFA starting from a given state. It uses the temporary field (tmp) of states to track the search path and identify cycles. When a loop is detected, it calls breakconstraintloop() to eliminate the loop and returns 1 to indicate success.

The algorithm works by:
1. **Stack overflow protection**: Checks recursion depth to prevent stack overflow
2. **Cycle detection**: Uses state tmp fields to detect when the search revisits a state in the current path
3. **Loop breaking**: Calls breakconstraintloop() when a cycle is found
4. **Memoization**: Marks states that don't lead to loops with tmp == s to avoid redundant searches

The function employs an optimization where states proven not to be part of any constraint loop are marked with s->tmp == s, allowing subsequent searches to skip them efficiently.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure being analyzed
- `s`: Starting state for the constraint loop search

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (macro to check recursion depth)
  - NERR (error reporting macro)
  - REG_ETOOBIG (error code for overly complex regex)
  - [breakconstraintloop](../b/breakconstraintloop.md) (function to break detected loops)
  - [isconstraintarc](../i/isconstraintarc.md) (checks if an arc is a constraint arc)
  - [findconstraintloop](findconstraintloop.md) (recursive self-call)
- Called from (representative examples):
  - [fixconstraintloops](fixconstraintloops.md) (main constraint loop fixing function)
  - [findconstraintloop](findconstraintloop.md) (recursive self-calls)

## Notes and Other Information
- This is a recursive function with potential for deep recursion in complex NFAs
- Uses state tmp fields both for cycle detection and memoization of negative results
- The found loop doesn't necessarily include the starting state - any reachable loop suffices
- Single-state loops are assumed to be already eliminated before this function is called
- Maximum recursion depth is bounded by the longest chain of constraint arcs in the NFA
- Returns 1 if a loop was found and broken, 0 if no loop exists from the starting state
- Tmp fields are guaranteed to be NULL on success return due to breakconstraintloop cleanup

## Simplified Source

```c
static int
findconstraintloop(struct nfa *nfa, struct state *s)
{
    struct arc *a;

    // Stack overflow protection for deep recursion
    if (STACK_TOO_DEEP(nfa->v->re)) {
        NERR(REG_ETOOBIG);
        return 1;
    }

    if (s->tmp != NULL) {
        // Already proven uninteresting?
        if (s->tmp == s)
            return 0;

        // Found a loop involving current state
        breakconstraintloop(nfa, s);
        return 1;
    }

    // Follow all constraint arcs to look for loops
    for (a = s->outs; a != NULL; a = a->outchain) {
        if (isconstraintarc(a)) {
            struct state *sto = a->to;

            assert(sto != s);  // Self-loops already eliminated
            s->tmp = sto;      // Mark our path

            if (findconstraintloop(nfa, sto))
                return 1;
        }
    }

    // No constraint loop found from this state
    s->tmp = s;  // Mark as proven uninteresting
    return 0;
}
```