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
  - shortest
  - longest
  - cdissect
  - LOFF
  - MDEBUG
  - ISERR
  - OFF
  - ERR
- Called from (representative examples):
  - cfind

## Notes and Other Information
- Implements nested loops: outer loop finds match ranges, inner loop tests positions
- Uses cdissect to verify that DFA matches satisfy all regex constraints
- Handles SHORTER flag for minimal vs maximal matching within each attempt
- Tracks cold start positions to optimize subsequent search operations
- Returns REG_OKAY on successful match, REG_NOMATCH if no match found
- Sets final match positions in v->pmatch[0] when successful
- The function represents the most complex part of the regex matching engine