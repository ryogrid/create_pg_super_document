# bracket

## Location
[src/backend/regex/regcomp.c:1673-1728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1673-L1728)

## Overview
Handles non-complemented bracket expressions in regular expression compilation, processing character classes and ranges within bracket notation.

## Definition

```c
static void
bracket(struct vars *v,
		struct state *lp,
		struct state *rp)
```
## Detailed Description
The  function processes non-complemented bracket expressions (like  or ) in regular expressions. It's also called from  for complemented bracket expressions (like ). The function parses the contents of bracket expressions, handling character classes, ranges, and complemented character classes. It uses a deferred processing approach for complemented character classes to avoid color bookkeeping confusion, storing them in a boolean array and processing them at the end. After processing all elements, it optimizes the bracket expression if complemented elements were found.

## Parameters / Member Variables
- : Pointer to the vars structure containing compilation state and NFA information
- : Pointer to the left/start state of the bracket expression
- : Pointer to the right/end state of the bracket expression

## Dependencies
- Functions called/Symbols referenced:
  - SEE (macro for checking current character)
  - NEXT (macro for advancing to next character)
  - EOS (end of string marker)
  - ISERR (macro for error checking)
  - [brackpart](brackpart.md) (processes individual parts of bracket expression)
  - [okcolors](../o/okcolors.md) (closes open subcolors)
  - NOERR (error handling macro)
  - [charclasscomplement](../c/charclasscomplement.md) (handles complemented character classes)
  - [optimizebracket](../o/optimizebracket.md) (optimizes bracket expression into rainbow if possible)
  - NUM_CCLASSES (constant for number of character classes)
- Called from:
  - ARCV (main arc processing function)
  - [cbracket](../c/cbracket.md) (complemented bracket expression handler)

## Notes and Other Information
- Uses deferred processing for complemented character classes to maintain proper color bookkeeping
- Maintains a boolean array  to track which complemented character classes were encountered
- Calls  only when complemented elements are present, as WHITE arcs can only result from complemented elements
- Located in src/backend/regex/regcomp.c:1673-1728