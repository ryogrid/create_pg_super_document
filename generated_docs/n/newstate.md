# newstate

## Location
[src/backend/regex/regc_nfa.c:137-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L137-L211)

## Overview
Allocates a new state in an NFA structure with efficient memory management using state batches and free lists.

## Definition

```c
static struct state *			/* NULL on error */
newstate(struct nfa *nfa)
```
## Detailed Description
The  function creates a new state for an NFA with sophisticated memory management. It first checks for interrupt signals to allow cancellation during compilation. The function uses a three-tier allocation strategy: first attempting to reuse states from a freelist, then using available space in the current state batch, and finally allocating a new batch when needed. State batches grow exponentially (doubling in size) up to a maximum limit to balance memory efficiency with allocation overhead. Each new state is initialized with a unique number, linked into the NFA's state list, and has all fields properly initialized.

## Parameters / Member Variables
- : Pointer to the NFA structure that will contain the new state

## Dependencies
- Functions called/Symbols referenced:
  - INTERRUPT
  - NERR
  - MALLOC
  - STATEBATCHSIZE
  - REG_MAX_COMPILE_SPACE
  - REG_ETOOBIG
  - REG_ESPACE
  - FIRSTSBSIZE
  - MAXSBSIZE
- Called from (representative examples):
  - [newnfa](newnfa.md) (in regc_nfa.c)
  - newfstate (in regc_nfa.c)
  - [duptraverse](../d/duptraverse.md) (in regc_nfa.c)
  - [pull](../p/pull.md) (in regc_nfa.c)
  - [push](../p/push.md) (in regc_nfa.c)
  - [makesearch](../m/makesearch.md) (in regcomp.c)
  - [parse](../p/parse.md) (in regcomp.c)
  - [parsebranch](../p/parsebranch.md) (in regcomp.c)

## Notes and Other Information
- Returns NULL on memory allocation failure or space limit exceeded
- Implements exponential growth strategy for state batches to optimize allocation
- Maintains both forward and backward linked lists for efficient state traversal
- Includes interrupt checking to support query cancellation during regex compilation
- Each state gets a unique sequential number for identification
- Properly initializes all state fields including ins/outs arrays and temporary pointers
- Critical function called frequently during NFA construction for complex regex patterns

## Simplified Source

```c
static struct state *newstate(struct nfa *nfa)
{
    struct state *s;

    // Check for operation cancellation
    INTERRUPT(nfa->v->re);

    // First, try to reuse a state from the freelist
    if (nfa->freestates != NULL) {
        s = nfa->freestates;
        nfa->freestates = s->next;
    }
    // Next, try to use space in current state batch
    else if (nfa->lastsb != NULL && nfa->lastsbused < nfa->lastsb->nstates) {
        s = &nfa->lastsb->s[nfa->lastsbused++];
    }
    // Finally, allocate a new state batch
    else {
        struct statebatch *newSb;
        size_t nstates;

        // Check space limit
        if (nfa->v->spaceused >= REG_MAX_COMPILE_SPACE) {
            NERR(REG_ETOOBIG);
            return NULL;
        }

        // Calculate batch size (exponential growth up to limit)
        nstates = (nfa->lastsb != NULL) ? nfa->lastsb->nstates * 2 : FIRSTSBSIZE;
        if (nstates > MAXSBSIZE)
            nstates = MAXSBSIZE;

        // Allocate new batch
        newSb = (struct statebatch *) MALLOC(STATEBATCHSIZE(nstates));
        if (newSb == NULL) {
            NERR(REG_ESPACE);
            return NULL;
        }

        // Update space tracking and link batch
        nfa->v->spaceused += STATEBATCHSIZE(nstates);
        newSb->nstates = nstates;
        newSb->next = nfa->lastsb;
        nfa->lastsb = newSb;
        nfa->lastsbused = 1;
        s = &newSb->s[0];
    }

    // Initialize the new state
    assert(nfa->nstates >= 0);
    s->no = nfa->nstates++;
    s->flag = 0;

    if (nfa->states == NULL)
        nfa->states = s;

    s->nins = 0;
    s->ins = NULL;
    s->nouts = 0;
    s->outs = NULL;
    s->tmp = NULL;
    s->next = NULL;

    // Link into state list
    if (nfa->slast != NULL) {
        assert(nfa->slast->next == NULL);
        nfa->slast->next = s;
    }
    s->prev = nfa->slast;
    nfa->slast = s;

    return s;
}
```