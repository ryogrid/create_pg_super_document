# push

## Location
[src/backend/regex/regc_nfa.c:1891-1986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1891-L1986)

## Overview
Pushes a forward constraint forward past its destination state in the PostgreSQL regex NFA (Non-deterministic Finite Automaton) implementation.

## Definition

```c
static int
push(struct nfa *nfa,
	 struct arc *con,
	 struct state **intermediates)
```
## Detailed Description
The  function is a core component of PostgreSQL's regex engine constraint optimization. It takes a forward constraint arc and propagates it through its destination state to subsequent states. This operation is essential for optimizing regular expression matching by moving constraints to more advantageous positions in the NFA.

The function maintains several important invariants:
- It never deletes pre-existing states
- It only removes the given constraint arc from the destination state's inarcs
- It may leave useless states behind, which are cleaned up by the calling  function
- It creates intermediate states as needed to handle multiple constraint combinations

The function returns 1 on success (which occurs unless the destination is the post state or an internal error occurs) and 0 if no operation was performed.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure being modified
- `*con`: The constraint arc to be pushed forward
- `**intermediates`: Pointer to a list of intermediate states for the destination state, chained through their tmp fields
## Dependencies
- Functions called/Symbols referenced:
  - [freearc](../f/freearc.md)
  - [newstate](../n/newstate.md)
  - NISERR
  - [copyouts](../c/copyouts.md)
  - [cparc](../c/cparc.md)
  - combine
  - [newarc](../n/newarc.md)
  - [moveouts](../m/moveouts.md)
  - INCOMPATIBLE, SATISFIED, COMPATIBLE, REPLACEARC (enum values)
  - NOTREACHED
- Called from:
  - [pushfwd](pushfwd.md)

## Notes and Other Information
- The function handles multiple scenarios when combining constraints with outgoing arcs: INCOMPATIBLE (destroys arc), SATISFIED (no action), COMPATIBLE (creates intermediate state), and REPLACEARC (replaces arc color)
- When the destination state has multiple input arcs, the function clones the state to avoid affecting other inarcs
- Intermediate states are reused when possible to avoid creating duplicate states for the same predecessor-successor combinations
- Part of the regex constraint optimization system in PostgreSQL's regex engine located in src/backend/regex/regc_nfa.c:1891-1986

## Simplified Source
```c
static int push(struct nfa *nfa, struct arc *con, struct state **intermediates) {
    struct state *from = con->from;
    struct state *to = con->to;
    struct arc *a, *nexta;
    struct state *s;

    // Can't push beyond post state or if destination is dead end
    if (to->flag) return 0;
    if (to->nouts == 0) {
        freearc(nfa, con);
        return 1;
    }

    // Clone state if it has multiple incoming arcs
    if (to->nins > 1) {
        s = newstate(nfa);
        if (NISERR()) return 0;
        copyouts(nfa, to, s);
        cparc(nfa, con, from, s);
        freearc(nfa, con);
        to = s;
        con = to->ins;
    }

    // Propagate constraint through outgoing arcs
    for (a = to->outs; a != NULL && !NISERR(); a = nexta) {
        nexta = a->outchain;
        switch (combine(nfa, con, a)) {
            case INCOMPATIBLE:
                freearc(nfa, a);
                break;
            case COMPATIBLE:
                // Find or create intermediate state
                for (s = *intermediates; s != NULL; s = s->tmp) {
                    if (s->ins->from == from && s->outs->to == a->to)
                        break;
                }
                if (s == NULL) {
                    s = newstate(nfa);
                    s->tmp = *intermediates;
                    *intermediates = s;
                }
                cparc(nfa, con, s, a->to);
                cparc(nfa, a, from, s);
                freearc(nfa, a);
                break;
            case REPLACEARC:
                newarc(nfa, a->type, con->co, from, a->to);
                freearc(nfa, a);
                break;
        }
    }

    // Move remaining arcs and cleanup
    moveouts(nfa, to, from);
    freearc(nfa, con);
    return 1;
}
```