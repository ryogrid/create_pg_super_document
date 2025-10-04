# dumpnfa

## Location
[src/backend/regex/regc_nfa.c:3646-3694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3646-L3694)

## Overview
A debugging function that outputs a human-readable representation of an NFA (Non-deterministic Finite Automaton) structure to a file stream for development and diagnostic purposes.

## Definition

```c
static void
dumpnfa(struct nfa *nfa,
		FILE *f)
```
## Detailed Description
The  function provides a comprehensive textual dump of an NFA structure, including all its states, arcs, and metadata. This function is only compiled when  is defined, making it a development and debugging tool rather than production code. It outputs detailed information about the NFA's structure including pre/post states, boundary conditions (beginning/end of string/line), flags, and statistics about the total number of states and arcs.

The function iterates through all states in the NFA, calling  for each one to provide detailed state information. It also displays color information if the NFA is a top-level (parent) NFA, and provides summary statistics about the total structure size.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure to be dumped
- `*f`: File stream where the dump output will be written
## Dependencies
- Functions called/Symbols referenced:
  - [dumpstate](dumpstate.md) (for dumping individual state information)
  - [dumpcolors](dumpcolors.md) (for dumping color map information)
  - COLORLESS (constant for colorless transitions)
  - HASLACONS (flag for lookahead/lookbehind constraints)
  - HASCANTMATCH (flag for cant-match constraints)
  - MATCHALL (flag for match-all patterns)
  - DUPINF (constant for infinite duplication)
- Called from (representative examples):
  - [optimize](../o/optimize.md) (src/backend/regex/regc_nfa.c:1612, 1631)
  - [pullback](../p/pullback.md) (src/backend/regex/regc_nfa.c:1678)
  - [pushfwd](../p/pushfwd.md) (src/backend/regex/regc_nfa.c:1849)
  - [fixempties](../f/fixempties.md) (src/backend/regex/regc_nfa.c:2284)
  - [fixconstraintloops](../f/fixconstraintloops.md) (src/backend/regex/regc_nfa.c:2446)

## Notes and Other Information
- This function is only available in debug builds (wrapped in )
- Used primarily for debugging regex compilation and optimization processes
- Provides detailed information about NFA flags including boundary conditions and constraint types
- Shows pre and post state numbers, which are critical anchor points in the NFA
- Includes statistics about total states and arcs for performance analysis
- Calls  to ensure output is immediately written to the file stream
- Part of PostgreSQL's regex engine debugging infrastructure

## Simplified Source

```c
static void
dumpnfa(struct nfa *nfa, FILE *f)
{
#ifdef REG_DEBUG
    struct state *s;
    int nstates = 0, narcs = 0;

    // Output NFA metadata and flags
    fprintf(f, "pre %d, post %d", nfa->pre->no, nfa->post->no);

    // Show boundary conditions if present
    if (nfa->bos[0] != COLORLESS)
        fprintf(f, ", bos [%ld]", (long) nfa->bos[0]);
    if (nfa->bos[1] != COLORLESS)
        fprintf(f, ", bol [%ld]", (long) nfa->bos[1]);
    if (nfa->eos[0] != COLORLESS)
        fprintf(f, ", eos [%ld]", (long) nfa->eos[0]);
    if (nfa->eos[1] != COLORLESS)
        fprintf(f, ", eol [%ld]", (long) nfa->eos[1]);

    // Show special flags
    if (nfa->flags & HASLACONS)
        fprintf(f, ", haslacons");
    if (nfa->flags & HASCANTMATCH)
        fprintf(f, ", hascantmatch");

    // Show match-all pattern limits if applicable
    if (nfa->flags & MATCHALL) {
        fprintf(f, ", minmatchall %d", nfa->minmatchall);
        if (nfa->maxmatchall == DUPINF)
            fprintf(f, ", maxmatchall inf");
        else
            fprintf(f, ", maxmatchall %d", nfa->maxmatchall);
    }

    fprintf(f, "\n");

    // Dump all states and count them
    for (s = nfa->states; s != NULL; s = s->next) {
        dumpstate(s, f);
        nstates++;
        narcs += s->nouts;
    }

    // Output summary statistics
    fprintf(f, "total of %d states, %d arcs\n", nstates, narcs);

    // Show color information for top-level NFAs
    if (nfa->parent == NULL)
        dumpcolors(nfa->cm, f);

    fflush(f);
#endif
}
```