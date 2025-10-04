# find

## Location
[src/backend/regex/regexec.c:419-508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L419-L508)

## Overview
Finds a match for the main NFA in the no-complications case during regex execution.

## Definition
```c
static int
find(struct vars *v, struct cnfa *cnfa, struct colormap *cm)
```

## Detailed Description
The `find` function implements the core pattern matching algorithm for regular expressions when there are no complications (such as backreferences or complex constraints). It uses a two-phase approach: first, it uses a search RE to quickly identify potential match ranges, then it uses the main NFA to find exact matches within those ranges. The function handles both shortest and longest match modes based on regex flags and supports the REG_EXPECT flag for extended match information. If submatches are required, it delegates to `cdissect` for detailed analysis.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing execution state and match results
- `cnfa`: Pointer to the compiled NFA (Non-deterministic Finite Automaton)
- `cm`: Pointer to the color map for character classification

## Dependencies
- Functions called/Symbols referenced:
  - [newdfa](../n/newdfa.md)
  - [shortest](../s/shortest.md)
  - [longest](../l/longest.md)
  - [freedfa](freedfa.md)
  - [cdissect](../c/cdissect.md)
  - LOFF
  - MDEBUG
  - NOERR
  - ISERR
  - OFF
- Called from (representative examples):
  - LOCALDFA execution path

## Notes and Other Information
- Returns REG_OKAY on successful match, REG_NOMATCH if no match found
- Uses two separate DFAs: one for search RE and one for main matching
- Implements SHORTER flag logic for minimal vs maximal matching
- Sets match positions in v->pmatch[0] for the overall match
- Handles REG_EXPECT flag by setting extended match information in v->details
- Delegates to cdissect for submatch extraction when v->nmatch > 1
- The function is static and part of the regex execution engine core

## Simplified Source

```c
static int find(struct vars *v, struct cnfa *cnfa, struct colormap *cm) {
    chr *begin, *end = NULL, *cold = NULL;
    chr *open, *close;
    int hitend;
    int shorter = (v->g->tree->flags & SHORTER) ? 1 : 0;

    // Phase 1: Use search RE to find potential match range
    struct dfa *s = newdfa(v, &v->g->search, cm, &v->dfa1);
    if (s == NULL)
        return v->err;

    close = shortest(v, s, v->search_start, v->search_start, v->stop,
                     &cold, (int *) NULL);
    freedfa(s);
    NOERR();

    // Handle REG_EXPECT flag
    if (v->g->cflags & REG_EXPECT) {
        if (cold != NULL)
            v->details->rm_extend.rm_so = OFF(cold);
        else
            v->details->rm_extend.rm_so = OFF(v->stop);
        v->details->rm_extend.rm_eo = OFF(v->stop);
    }

    if (close == NULL)  // No match found
        return REG_NOMATCH;

    if (v->nmatch == 0)  // Match found, no details needed
        return REG_OKAY;

    // Phase 2: Find exact match within potential range
    open = cold;
    cold = NULL;

    struct dfa *d = newdfa(v, cnfa, cm, &v->dfa1);
    if (d == NULL)
        return v->err;

    // Try each position in the range
    for (begin = open; begin <= close; begin++) {
        if (shorter)
            end = shortest(v, d, begin, begin, v->stop, (chr **) NULL, &hitend);
        else
            end = longest(v, d, begin, v->stop, &hitend);

        if (ISERR()) {
            freedfa(d);
            return v->err;
        }

        if (hitend && cold == NULL)
            cold = begin;

        if (end != NULL)
            break;  // Found a match
    }

    freedfa(d);

    // Record match positions
    v->pmatch[0].rm_so = OFF(begin);
    v->pmatch[0].rm_eo = OFF(end);

    // Update extended info for REG_EXPECT
    if (v->g->cflags & REG_EXPECT) {
        if (cold != NULL)
            v->details->rm_extend.rm_so = OFF(cold);
        else
            v->details->rm_extend.rm_so = OFF(v->stop);
        v->details->rm_extend.rm_eo = OFF(v->stop);
    }

    if (v->nmatch == 1)  // No submatches needed
        return REG_OKAY;

    // Extract submatches
    return cdissect(v, v->g->tree, begin, end);
}
```