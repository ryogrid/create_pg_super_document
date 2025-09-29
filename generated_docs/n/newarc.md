# newarc

## Location
[src/backend/regex/regc_nfa.c:281-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L281-L322)

## Overview
Creates a new arc within an NFA (Non-deterministic Finite Automaton) while ensuring no duplicate arcs are created during regex compilation.

## Definition

```c
static void
newarc(struct nfa *nfa,
	   int t,
	   color co,
	   struct state *from,
	   struct state *to)
```
## Detailed Description
The newarc function is responsible for setting up a new arc within an NFA structure used in PostgreSQL's regex engine. It performs duplicate checking to ensure that no redundant arcs are created, which is important for maintaining NFA efficiency. The function uses an optimization strategy where it checks for duplicates using whichever chain (incoming or outgoing) is shorter to minimize search time. If no duplicate is found, it delegates the actual arc creation to the createarc function.

The function also includes an interrupt check point to allow for operation cancellation during regex compilation, since regex compilation can involve creating many states and arcs.

## Parameters / Member Variables
- : Pointer to the NFA structure that will contain the new arc
- : The type of the arc being created
- : The color associated with the arc (used in regex character classification)
- : Pointer to the source state of the arc
- : Pointer to the destination state of the arc

## Dependencies
- Functions called/Symbols referenced:
  - INTERRUPT (for operation cancellation checks)
  - [createarc](../c/createarc.md) (for actual arc creation)
- Called from (representative examples):
  - [subcolorcvec](../s/subcolorcvec.md) (color vector processing)
  - [newnfa](newnfa.md) (NFA initialization)
  - [cparc](../c/cparc.md) (arc copying)
  - [makesearch](../m/makesearch.md) (search pattern creation)
  - [cbracket](../c/cbracket.md) (bracket expression processing)

## Notes and Other Information
- The function includes a comment noting that RAINBOW arcs are theoretically redundant with plain arcs (except for pseudocolors), but this redundancy is not optimized away due to complexity considerations
- The duplicate checking algorithm chooses the shorter chain (from->nouts vs to->nins) for efficiency
- This function is static and only used within the regex NFA compilation module
- The function serves as a key interrupt point for long-running regex compilation operations

## Simplified Source

```c
static void newarc(struct nfa *nfa,
                  int t,
                  color co,
                  struct state *from,
                  struct state *to)
{
    struct arc *a;

    assert(from != NULL && to != NULL);

    // Check for operation cancellation
    INTERRUPT(nfa->v->re);

    // Check for duplicate arc using shorter chain for efficiency
    if (from->nouts <= to->nins) {
        // Search outgoing arcs from source state
        for (a = from->outs; a != NULL; a = a->outchain) {
            if (a->to == to && a->co == co && a->type == t)
                return; // Duplicate found, don't create
        }
    } else {
        // Search incoming arcs to destination state
        for (a = to->ins; a != NULL; a = a->inchain) {
            if (a->from == from && a->co == co && a->type == t)
                return; // Duplicate found, don't create
        }
    }

    // No duplicate found, create the arc
    createarc(nfa, t, co, from, to);
}
```