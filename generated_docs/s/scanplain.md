# scanplain

## Location
[src/backend/regex/regcomp.c:1886-1910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1886-L1910)

## Overview
Scans the PLAIN contents within bracket expression delimiters like [. .], [= =], or [: :], returning a pointer to the end of the sequence.

## Definition
```c
static const chr *scanplain(struct vars *v)
```

## Detailed Description
The `scanplain` function is a utility that scans through plain character sequences within special bracket expression constructs such as collating elements ([. .]), equivalence classes ([= =]), and character classes ([: :]). It starts after the opening delimiter, advances through all PLAIN tokens, and stops at the closing delimiter. The function is designed to work with the lexer in regc_lex.c and specifically does not attempt to look past the final bracket of these constructs. It returns a pointer to just after the end of the plain character sequence, which can then be used to extract the content between the delimiters.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing parsing state and current position

## Dependencies
- Functions called/Symbols referenced:
  - SEE (macro for checking current token type)
  - NEXT (macro for advancing to next token)
  - ISERR (macro for error checking)
  - Token type constants: COLLEL, ECLASS, CCLASS, PLAIN, END
- Called from:
  - [brackpart](../b/brackpart.md) (called multiple times to scan different bracket constructs)

## Notes and Other Information
- Works specifically with bracket expression delimited constructs: [. .], [= =], [: :]
- Designed to cooperate with lexer logic in regc_lex.c
- Does not look past the final bracket of the constructs it scans
- Returns pointer to position just after the plain content, before the closing delimiter
- Used for extracting content from collating elements, equivalence classes, and character classes
- Located in src/backend/regex/regcomp.c:1886-1910