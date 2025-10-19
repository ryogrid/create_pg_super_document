# lexstart

## Location
[src/backend/regex/regc_lex.c:70-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_lex.c#L70-L98)

## Overview
Initializes lexical analysis setup and scans leading options for regular expression compilation, setting up the appropriate lexical context based on the regex flags.

## Definition
```c
static void lexstart(struct vars *v)
```

## Detailed Description
The `lexstart` function is responsible for initializing the lexical analysis phase of regular expression compilation. It first processes any prefix options that may affect compilation behavior, then determines the appropriate lexical context based on the compilation flags (cflags) in the vars structure. The function sets the lexical context to one of three modes: quoted (L_Q), extended regular expression (L_ERE), or basic regular expression (L_BRE). After establishing the context, it initializes the token stream by setting up the first token for subsequent parsing.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing compilation state, including cflags (compilation flags), nexttype (next token type), and other lexical analysis state

## Dependencies
- Functions called/Symbols referenced:
  - [prefixes](../p/prefixes.md)
  - NOERR
  - INTOCON
  - [next](../n/next.md)
- Constants referenced:
  - REG_QUOTE, REG_EXTENDED, REG_ADVANCED, REG_EXPANDED, REG_NEWLINE, REG_ADVF
  - L_Q, L_ERE, L_BRE
  - EMPTY
- Called from (representative examples):
  - CNOERR (in regcomp.c)

## Notes and Other Information
The function uses assertions to ensure mutually exclusive flag combinations are not set simultaneously. The three lexical contexts correspond to different regex syntax modes: quoted mode treats all characters literally, extended mode supports modern regex features, and basic mode provides traditional regex functionality. The function ensures proper initialization of the token stream by calling next() to prepare the first token for the parser.

## Simplified Source

```c
static void lexstart(struct vars *v) {
    // Process any prefix options that may affect compilation
    prefixes(v);
    NOERR();

    // Set lexical context based on compilation flags
    if (v->cflags & REG_QUOTE) {
        // Quoted mode - treat all characters literally
        INTOCON(L_Q);
    } else if (v->cflags & REG_EXTENDED) {
        // Extended regular expression mode
        INTOCON(L_ERE);
    } else {
        // Basic regular expression mode
        INTOCON(L_BRE);
    }

    // Initialize token stream
    v->nexttype = EMPTY;
    next(v);  // Set up the first token
}
```