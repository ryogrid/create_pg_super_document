# cfindloop

## Location
[src/backend/regex/regexec.c:549-662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L549-L662)

## Overview
The core matching engine for complex regex patterns with complications like backreferences.

## Definition
```c
static int
cfindloop(struct vars *v, struct cnfa *cnfa, struct colormap *cm,
          struct dfa *d, struct dfa *s, chr **coldp)
```

## Detailed Description
The `cfindloop` function implements the heart of the complex regex matching algorithm when complications are present. It uses a sophisticated two-level search strategy: first using a search DFA to identify potential match ranges, then systematically testing each position within those ranges using the main DFA. For each potential match found by the DFA, it calls `cdissect` to verify that the match satisfies all regex constraints including backreferences. The function handles both shortest and longest match modes and tracks cold start positions for performance optimization in subsequent searches.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing execution state
- `cnfa`: Pointer to the compiled NFA for detailed matching
- `cm`: Pointer to the color map for character classification
- `d`: Main DFA for detailed pattern matching
- `s`: Search DFA for finding potential match ranges
- `coldp`: Output parameter for cold start pointer (optimization hint)

## Dependencies
- Functions called/Symbols referenced:
  - [shortest](../s/shortest.md)
  - [longest](../l/longest.md)
  - [cdissect](cdissect.md)
  - LOFF
  - MDEBUG
  - ISERR
  - OFF
  - ERR
- Called from (representative examples):
  - [cfind](cfind.md)

## Notes and Other Information
- Implements nested loops: outer loop finds match ranges, inner loop tests positions
- Uses cdissect to verify that DFA matches satisfy all regex constraints
- Handles SHORTER flag for minimal vs maximal matching within each attempt
- Tracks cold start positions to optimize subsequent search operations
- Returns REG_OKAY on successful match, REG_NOMATCH if no match found
- Sets final match positions in v->pmatch[0] when successful
- The function represents the most complex part of the regex matching engine

## Simplified Source

```c
static int cfindloop(struct vars *v, struct cnfa *cnfa, struct colormap *cm,
                     struct dfa *d, struct dfa *s, chr **coldp) {
    chr *begin, *end, *cold = NULL;
    chr *open, *close, *estart, *estop;
    int er;
    int shorter = v->g->tree->flags & SHORTER;
    int hitend;

    close = v->search_start;

    // Main search loop - find match ranges and test each position
    do {
        // Phase 1: Use search DFA to find potential match range
        close = shortest(v, s, close, close, v->stop, &cold, (int *) NULL);
        if (ISERR()) {
            *coldp = cold;
            return v->err;
        }

        if (close == NULL)
            break; // No more potential matches

        open = cold;
        cold = NULL;

        // Phase 2: Test each position in the potential range
        for (begin = open; begin <= close; begin++) {
            estart = begin;
            estop = v->stop;

            // Inner loop: try different match lengths at this position
            for (;;) {
                // Find potential match end using main DFA
                if (shorter)
                    end = shortest(v, d, begin, estart, estop, (chr **) NULL, &hitend);
                else
                    end = longest(v, d, begin, estop, &hitend);

                if (ISERR()) {
                    *coldp = cold;
                    return v->err;
                }

                if (hitend && cold == NULL)
                    cold = begin;

                if (end == NULL)
                    break; // No match at this position

                // Phase 3: Verify match satisfies all constraints
                er = cdissect(v, v->g->tree, begin, end);

                if (er == REG_OKAY) {
                    // Found valid match
                    if (v->nmatch > 0) {
                        v->pmatch[0].rm_so = OFF(begin);
                        v->pmatch[0].rm_eo = OFF(end);
                    }
                    *coldp = cold;
                    return REG_OKAY;
                }

                if (er != REG_NOMATCH) {
                    ERR(er);
                    *coldp = cold;
                    return er;
                }

                // Try next match length at same position
                if (shorter) {
                    if (end == estop)
                        break;
                    estart = end + 1;
                } else {
                    if (end == begin)
                        break;
                    estop = end - 1;
                }
            }
        }

        // Move to next potential match range
        close++;
    } while (close < v->stop);

    *coldp = cold;
    return REG_NOMATCH;
}
```