# brenext

## Location
[src/backend/regex/regc_lex.c:861-981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_lex.c#L861-L981)

## Overview
The  function is a lexical analyzer component that tokenizes Basic Regular Expression (BRE) syntax, handling the context-dependent interpretation of special characters and backslash escapes.

## Definition

```c
static int						/* 1 normal, 0 failure */
brenext(struct vars *v,
		chr c)
```
## Detailed Description
The  function processes the next token in a Basic Regular Expression string, implementing BRE-specific parsing rules that differ from Extended Regular Expressions (ERE). It handles context-dependent meanings of metacharacters like , , and , as well as backslash escape sequences. The function returns 1 for normal operation and 0 for failure, using various macros to set token types and values.

The function implements two main parsing phases:
1. Direct character interpretation (switch on input character)
2. Backslash escape sequence processing (when c == '\')

Key BRE-specific behaviors include:
-  is literal when it appears at the beginning, after , or after 
-  is an anchor only at the beginning or after 
-  is an anchor only at the end or before 
- Bracket expressions  for word boundaries
- Numbered backreferences  through 

## Parameters / Member Variables
- `*v`: Pointer to the regex parsing state structure containing the current position, flags, and context
- `c`: The current character being processed from the input string
## Dependencies
- Functions called/Symbols referenced:
  - LASTTYPE (macro for checking previous token type)
  - RETV/RET (macros for returning token values)
  - HAVE/NEXT1/NEXT2 (macros for lookahead)
  - INTOCON (macro for entering lexical contexts)
  - [skip](../s/skip.md) (function for skipping whitespace)
  - ATEOS (macro for end-of-string check)
  - NOTE (macro for recording regex features used)
  - FAILW (macro for error handling)
- Called from (representative examples):
  - [next](../n/next.md) (main tokenizer dispatch function)

## Notes and Other Information
- Part of PostgreSQL's regex engine implementation in src/backend/regex/regc_lex.c:861-981
- Handles numerous PostgreSQL-specific regex extensions and POSIX compliance features
- Uses extensive macro-based error handling and token generation
- Implements complex context-sensitivity required by BRE syntax rules
- Records usage of non-standard regex features through NOTE() calls for compatibility warnings

## Simplified Source

```c
static int
brenext(struct vars *v, chr c)
{
    // Handle non-backslash characters first
    switch (c) {
        case '*':
            // Context-dependent: literal at beginning, after '(', or after '^'
            if (LASTTYPE(EMPTY) || LASTTYPE('(') || LASTTYPE('^')) {
                RETV(PLAIN, c);
            }
            RETV('*', 1);

        case '[':
            // Check for special word boundary sequences [[:<:]] and [[:>:]]
            if (HAVE(6) && *(v->now + 0) == '[' && *(v->now + 1) == ':' &&
                (*(v->now + 2) == '<' || *(v->now + 2) == '>') &&
                *(v->now + 3) == ':' && *(v->now + 4) == ']' && *(v->now + 5) == ']') {
                c = *(v->now + 2);
                v->now += 6;
                NOTE(REG_UNONPOSIX);
                RET((c == '<') ? '<' : '>');
            }
            // Regular bracket expression
            INTOCON(L_BRACK);
            RETV('[', NEXT1('^') ? (v->now++, 0) : 1);

        case '.':
            RET('.');

        case '^':
            // Context-dependent: anchor only at beginning or after '('
            if (LASTTYPE(EMPTY) || LASTTYPE('(')) {
                RET('^');
            }
            RETV(PLAIN, c);

        case '$':
            // Context-dependent: anchor only at end or before '\)'
            if (v->cflags & REG_EXPANDED) skip(v);
            if (ATEOS() || NEXT2('\\', ')')) {
                RET('$');
            }
            RETV(PLAIN, c);

        case '\\':
            break;  // Handle below

        default:
            RETV(PLAIN, c);
    }

    // Handle backslash escape sequences
    if (ATEOS()) FAILW(REG_EESCAPE);

    c = *v->now++;
    switch (c) {
        case '{': INTOCON(L_BBND); RET('{');       // Begin bound
        case '(': RETV('(', 1);                    // Open group
        case ')': RETV(')', c);                    // Close group
        case '<': NOTE(REG_UNONPOSIX); RET('<');   // Word boundary start
        case '>': NOTE(REG_UNONPOSIX); RET('>');   // Word boundary end

        case '1': case '2': case '3': case '4': case '5':
        case '6': case '7': case '8': case '9':
            // Backreferences
            NOTE(REG_UBACKREF);
            RETV(BACKREF, (chr) DIGITVAL(c));

        default:
            // Other escaped characters are literal
            if (iscalnum(c)) {
                NOTE(REG_UBSALNUM);
                NOTE(REG_UUNSPEC);
            }
            RETV(PLAIN, c);
    }

    return 1;
}
```