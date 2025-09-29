# matchuntil

## Location
[src/backend/regex/rege_dfa.c:371-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L371-L505)

## Overview
Implements an incremental matching engine for search-style NFAs that determines match existence with O(N) time complexity across multiple calls.

## Definition
```c
static int
matchuntil(struct vars *v,
           struct dfa *d,
           chr *probe,          /* we want to know if a match ends here */
           struct sset **lastcss, /* state storage across calls */
           chr **lastcp)        /* state storage across calls */
```

## Detailed Description
The `matchuntil` function is designed for incremental regex matching with search-style NFAs (patterns that behave as if they had a leading `.*`). It efficiently determines whether a match exists starting at v->start and ending at the probe position. The key innovation is that multiple calls with non-decreasing probe values require only O(N) time total, not O(N²), by maintaining state between calls. This makes it highly efficient for scanning operations where you need to check match endings at multiple positions.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and state
- `d`: Pointer to DFA structure containing the compiled search automaton
- `probe`: Target ending position to test for match completion
- `lastcss`: Pointer to state set storage that persists across multiple calls
- `lastcp`: Pointer to character position storage that persists across multiple calls

## Dependencies
- Functions called/Symbols referenced:
  - [initialize](../i/initialize.md) (for setting up initial DFA state when needed)
  - [miss](miss.md) (for handling DFA state transitions)
  - GETCOLOR (for character-to-color mapping)
  - FDEBUG (for debug tracing)
  - MATCHALL, DUPINF (for MATCHALL NFA optimization)
- Called from (representative examples):
  - [lacon](../l/lacon.md) (lookahead/lookbehind constraint processing)
  - LOFF (regex execution offset function)

## Notes and Other Information
- Optimized for search patterns with leading .* behavior
- Maintains persistent state between calls for O(N) amortized performance
- Includes fast path for MATCHALL NFAs with direct character counting
- Supports both normal and traced execution modes
- Returns 1 for match found, 0 for no match or internal error
- Critical for efficient implementation of lookahead/lookbehind assertions in PostgreSQL regex engine

## Simplified Source

```c
static int
matchuntil(struct vars *v, struct dfa *d, chr *probe,
           struct sset **lastcss, chr **lastcp)
{
    chr *cp = *lastcp;
    struct sset *css = *lastcss;
    struct sset *ss;
    struct colormap *cm = d->cm;

    // Fast path for MATCHALL NFAs - just count characters
    if (d->cnfa->flags & MATCHALL) {
        size_t nchr = probe - v->start;
        if (nchr < d->cnfa->minmatchall)
            return 0;
        return 1;  // maxmatchall is always infinite
    }

    // Initialize or restart if needed
    if (cp == NULL || cp > probe) {
        cp = v->start;
        css = initialize(v, d, cp);
        if (css == NULL)
            return 0;

        // Handle beginning-of-string transition
        color co = d->cnfa->bos[(v->eflags & REG_NOTBOL) ? 0 : 1];
        css = miss(v, d, css, co, cp, v->start);
        if (css == NULL)
            return 0;
        css->lastseen = cp;
    } else if (css == NULL) {
        return 0;  // Previously determined no match possible
    }

    // Main character scanning loop
    while (cp < probe) {
        color co = GETCOLOR(cm, *cp);
        ss = css->outs[co];
        if (ss == NULL) {
            ss = miss(v, d, css, co, cp + 1, v->start);
            if (ss == NULL)
                break;
        }
        cp++;
        ss->lastseen = cp;
        css = ss;
    }

    *lastcss = css;
    *lastcp = cp;

    if (css == NULL)
        return 0;

    // Process final character or end-of-string
    color co;
    if (cp < v->stop) {
        co = GETCOLOR(cm, *cp);
        ss = css->outs[co];
        if (ss == NULL)
            ss = miss(v, d, css, co, cp + 1, v->start);
    } else {
        co = d->cnfa->eos[(v->eflags & REG_NOTEOL) ? 0 : 1];
        ss = miss(v, d, css, co, cp, v->start);
    }

    // Check if we reached a match state
    if (ss == NULL || !(ss->flags & POSTSTATE))
        return 0;

    return 1;
}
```