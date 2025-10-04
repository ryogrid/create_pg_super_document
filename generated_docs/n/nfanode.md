# nfanode

## Location
[src/backend/regex/regcomp.c:2351-2390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2351-L2390)

## Overview
Processes a single NFA node in the regular expression parse tree, creating an optimized NFA fragment from a subre (sub-regular expression) structure.

## Definition

```c
static long						/* optimize results */
nfanode(struct vars *v,
		struct subre *t,
		int converttosearch,
		FILE *f)				/* for debug output */
```
## Detailed Description
The nfanode function is a core component of PostgreSQL's regex compilation process that converts a single node in the parsed regular expression tree into an optimized NFA (Non-deterministic Finite Automaton). It takes a subre (sub-regular expression) structure representing a portion of the regex parse tree and builds a complete NFA fragment from it.

The function performs several key operations:
1. Creates a new NFA using the provided color map and parent NFA context
2. Duplicates the NFA structure from the subre's begin/end states into the new NFA
3. Applies special color handling for optimization
4. Runs NFA optimization to improve performance
5. Optionally converts the NFA to a search NFA (when converttosearch is true)
6. Compacts the NFA into a compressed representation

This function is essential for the regex compilation pipeline, transforming high-level regex syntax into efficient automata that can be executed for pattern matching.

## Parameters / Member Variables
- `*v`: vars structure containing compilation context, color maps, and error state
- `*t`: subre structure representing the parse tree node to process into an NFA
- `converttosearch`: boolean flag indicating whether to apply makesearch() conversion
- `*f`: FILE pointer for debug output (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [newnfa](newnfa.md) - Creates a new NFA structure
  - [dupnfa](../d/dupnfa.md) - Duplicates NFA states and transitions
  - [specialcolors](../s/specialcolors.md) - Handles special color processing for optimization
  - [optimize](../o/optimize.md) - Performs NFA optimization
  - [makesearch](../m/makesearch.md) - Converts NFA to search NFA format
  - [compact](../c/compact.md) - Compresses NFA into final representation
  - [freenfa](../f/freenfa.md) - Deallocates NFA memory
  - NOERR/ISERR - Error checking macros
- Called from (representative examples):
  - nfatree - Main tree processing function
  - CNOERR - Error handling context

## Notes and Other Information
- Returns optimization results as a long value
- Handles debug output when FILE pointer is provided
- Part of the regex compilation pipeline that transforms parse trees into executable NFAs
- The converttosearch parameter allows selective application of search optimization
- Memory management is handled through freenfa() cleanup
- Error state is managed through the vars structure and checked at each major step

## Simplified Source

```c
static long nfanode(struct vars *v, struct subre *t, int converttosearch, FILE *f) {
    struct nfa *nfa;
    long ret = 0;

    // Create new NFA from parent context
    nfa = newnfa(v, v->cm, v->nfa);
    if (ISERR()) return ret;

    // Copy structure from subre to new NFA
    dupnfa(nfa, t->begin, t->end, nfa->init, nfa->final);
    nfa->flags = v->nfa->flags;

    // Apply optimizations step by step
    if (!ISERR()) specialcolors(nfa);
    if (!ISERR()) ret = optimize(nfa, f);

    // Optional search conversion
    if (converttosearch && !ISERR())
        makesearch(v, nfa);

    // Compact into final form
    if (!ISERR())
        compact(nfa, &t->cnfa);

    freenfa(nfa);
    return ret;
}
```