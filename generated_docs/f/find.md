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
  - newdfa
  - shortest
  - longest
  - freedfa
  - cdissect
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