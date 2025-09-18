# brackpart

## Location
src/backend/regex/regcomp.c: 1763 - 1885

## Overview
Handles one item or range within a bracket expression, parsing various bracket element types and creating appropriate NFA arcs.

## Definition
```c
static void brackpart(struct vars *v, struct state *lp, struct state *rp, bool *have_cclassc)
```

## Detailed Description
The `brackpart` function processes individual components within bracket expressions, handling various element types including plain characters, ranges, collating elements, equivalence classes, character classes, and complemented character classes. It uses a switch statement to handle different token types (PLAIN, RANGE, COLLEL, ECLASS, CCLASS, CCLASSS, CCLASSC). For ranges, it processes both start and end characters and creates a range using the `range` function. The function includes special handling for complemented character classes by marking them in the `have_cclassc` array for deferred processing. It also includes portability warnings for character ranges since they may not be portable across different character encodings.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing compilation state and current parsing position
- `lp`: Pointer to the left/start state for the bracket part
- `rp`: Pointer to the right/end state for the bracket part  
- `have_cclassc`: Boolean array tracking which complemented character classes were encountered

## Dependencies
- Functions called/Symbols referenced:
  - ERR (error reporting macro)
  - NEXT (advances to next token)
  - SEE (checks current token type)
  - [onechr](../o/onechr.md) (handles single character)
  - [scanplain](../s/scanplain.md) (scans plain text within delimiters)
  - INSIST (assertion with error handling)
  - [element](../e/element.md) (processes collating elements)
  - [eclass](../e/eclass.md) (handles equivalence classes)
  - [subcolorcvec](../s/subcolorcvec.md) (creates arcs for character vector)
  - lookupcclass (looks up character class)
  - [charclass](../c/charclass.md) (handles character classes)
  - [range](../r/range.md) (creates character ranges)
  - NOERR/NOTE (error handling macros)
  - Various constants: REG_ERANGE, REG_ECOLLATE, REG_ECTYPE, REG_ASSERT, REG_ICASE, REG_UUNPORT
- Called from:
  - [bracket](bracket.md) (main bracket expression handler)

## Notes and Other Information
- Supports multiple bracket element types: plain chars, ranges, collating elements, equivalence classes, character classes
- Defers processing of complemented character classes to avoid color bookkeeping issues
- Includes portability warnings for character ranges (REG_UUNPORT)
- Handles both case-sensitive and case-insensitive matching via REG_ICASE flag
- Contains extensive error checking for malformed bracket expressions
- Located in src/backend/regex/regcomp.c:1763-1885