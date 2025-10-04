# markcanreach

## Location
[src/backend/regex/regc_nfa.c:3025-3050](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3025-L3050)

## Overview
The markcanreach function is a recursive utility function in PostgreSQL's regex engine that marks all states from which a given target state can be reached within an NFA (Nondeterministic Finite Automaton).

## Definition

```c
static void
markcanreach(struct nfa *nfa,
			 struct state *s,
			 struct state *okay,	/* consider only states with this mark */
			 struct state *mark)	/* the value to mark with */
```
## Detailed Description
This function performs a reverse depth-first traversal of an NFA, starting from a given state and working backwards through incoming arcs to mark all states that can reach the starting state. It only considers states that currently have the 'okay' mark and changes them to the new 'mark' value. The function follows all incoming arcs (ins) from each state, tracing backwards through the NFA structure.

Like its companion function markreachable, this function includes stack overflow protection using the STACK_TOO_DEEP macro to prevent stack exhaustion when processing very complex regex patterns with deep NFA structures.

The marking mechanism uses the tmp field of state structures to track which states have been processed, ensuring each state is marked only once and preventing infinite loops in cyclic NFAs.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure being processed
- `*s`: The target state from which to trace backwards and mark reachable sources
- `*okay`: Only states with this mark value will be considered for processing
- `*mark`: The new mark value to assign to states that can reach the target
## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (stack overflow protection macro)
  - NERR (error reporting macro)
  - REG_ETOOBIG (error code for regex too complex)
  - [markcanreach](markcanreach.md) (recursive self-call)
- Called from (representative examples):
  - [cleanup](../c/cleanup.md) (src/backend/regex/regc_nfa.c:2976)
  - REPLACEARC macro (src/backend/regex/regcomp.c:222)

## Notes and Other Information
- This is a static function, only accessible within the regc_nfa.c file
- Complementary to markreachable, this function traces backwards through incoming arcs instead of forward through outgoing arcs
- The function is tail-recursive and includes stack overflow protection
- Uses the tmp field of state structures for marking purposes
- Critical for NFA optimization, particularly for identifying unreachable code paths and dead states
- Essential for regex engine cleanup operations and optimization phases

## Simplified Source

```c
static void
markcanreach(struct nfa *nfa, struct state *s, struct state *okay, struct state *mark)
{
    struct arc *a;

    // Stack overflow protection for deep recursion
    if (STACK_TOO_DEEP(nfa->v->re)) {
        NERR(REG_ETOOBIG);
        return;
    }

    // Only process states with the 'okay' mark
    if (s->tmp != okay)
        return;

    // Mark this state with the new mark value
    s->tmp = mark;

    // Recursively mark all states that can reach this one via incoming arcs
    for (a = s->ins; a != NULL; a = a->inchain)
        markcanreach(nfa, a->from, okay, mark);
}
```