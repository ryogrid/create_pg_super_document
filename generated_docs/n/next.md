# next

## Location
[src/backend/regex/regc_lex.c:200-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_lex.c#L200-L600)

## Overview
The main lexical analysis function that retrieves the next token from the regular expression input stream, handling different lexical contexts and character interpretations based on the current regex mode.

## Definition
```c
static int next(struct vars *v)
```

## Detailed Description
The `next` function is the core lexical analyzer for regular expression compilation. It processes input characters according to the current lexical context (stored in `v->lexcon`) and returns appropriate tokens for the parser. The function handles multiple regex modes including Basic Regular Expressions (BRE), Extended Regular Expressions (ERE), Advanced Regular Expressions (ARE), and literal strings.

Key functionality includes:
- **Context-sensitive parsing**: Different behavior based on lexical context (L_ERE, L_BRE, L_Q, L_BRACK, etc.)
- **Token generation**: Converts character sequences into meaningful tokens (PLAIN, operators, brackets, etc.)
- **Special character handling**: Processes metacharacters like `*`, `+`, `?`, `{`, `}`, `[`, `]`, `(`, `)`, `.`, `^`, `$`, `|`
- **Escape sequence processing**: Handles backslash escapes through the `lexescape` function
- **Advanced features**: Supports lookahead/lookbehind assertions, non-capturing groups, and comments in ARE mode
- **Error handling**: Reports various lexical errors (REG_EBRACE, REG_EBRACK, etc.)
- **Whitespace skipping**: Handles expanded mode where whitespace is ignored in appropriate contexts

The function uses a restart mechanism for handling comments and maintains state about the last token type for context-dependent parsing.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing:
  - `lexcon`: Current lexical context (L_ERE, L_BRE, L_Q, L_BRACK, etc.)
  - `cflags`: Compilation flags affecting lexical behavior
  - `now`: Current position in input string
  - `nexttype`: Type of the next token to be returned
  - `lasttype`: Type of the previously processed token

## Dependencies
- Functions called/Symbols referenced:
  - [skip](../s/skip.md), brenext, lexescape
  - ISERR, ATEOS, HAVE, NEXT1, NEXT2, NEXT3, LASTTYPE, INCON, INTOCON
  - RET, RETV, FAILW, NOTE, ERR
  - CHR, DIGITVAL, iscdigit, iscalnum
- Constants referenced:
  - Token types: EMPTY, EOS, PLAIN, DIGIT, RANGE, COLLEL, ECLASS, CCLASS, END, LACON
  - Lexical contexts: L_ERE, L_BRE, L_Q, L_BRACK, L_CEL, L_ECL, L_CCL, L_EBND, L_BBND
  - Regex flags: REG_BOSONLY, REG_EXPANDED, REG_ADVF, REG_EXTENDED
  - Error codes: REG_EBRACE, REG_EBRACK, REG_EESCAPE, REG_BADBR, REG_BADRPT, REG_BADOPT
  - Lookaround types: LATYPE_AHEAD_POS, LATYPE_AHEAD_NEG, LATYPE_BEHIND_POS, LATYPE_BEHIND_NEG
- Called from (representative examples):
  - [lexstart](../l/lexstart.md) (sets up first token)
  - Various parsing functions during regex compilation

## Notes and Other Information
This function is central to the regex lexical analysis phase and must handle the complexity of different regex syntaxes. It includes special handling for word boundaries (`[[:<:]]` and `[[:>:]]`), non-greedy quantifiers (`*?`, `+?`, `??`), and advanced features like lookaround assertions. The function maintains careful state management to ensure proper context transitions, especially when entering and exiting bracket expressions and bound contexts. Error reporting is comprehensive, covering malformed brackets, braces, escapes, and other syntax violations.

## Simplified Source

```c
static int
next(struct vars *v)
{
    chr c;

next_restart:
    // Check for errors or special start conditions
    if (ISERR()) return 0;

    v->lasttype = v->nexttype;

    // Handle REG_BOSONLY flag
    if (v->nexttype == EMPTY && (v->cflags & REG_BOSONLY)) {
        RETV(SBEGIN, 0);
    }

    // Skip whitespace in expanded mode
    if (v->cflags & REG_EXPANDED) {
        switch (v->lexcon) {
            case L_ERE: case L_BRE: case L_EBND: case L_BBND:
                skip(v);
                break;
        }
    }

    // Handle end of string based on context
    if (ATEOS()) {
        switch (v->lexcon) {
            case L_ERE: case L_BRE: case L_Q:
                RET(EOS);
            case L_EBND: case L_BBND:
                FAILW(REG_EBRACE);
            case L_BRACK: case L_CEL: case L_ECL: case L_CCL:
                FAILW(REG_EBRACK);
        }
    }

    c = *v->now++;

    // Handle different lexical contexts
    switch (v->lexcon) {
        case L_BRE:
            return brenext(v, c);

        case L_Q:  // Literal strings
            RETV(PLAIN, c);

        case L_EBND: case L_BBND:  // Bound contexts
            if (c >= '0' && c <= '9') {
                RETV(DIGIT, (chr) DIGITVAL(c));
            } else if (c == ',') {
                RET(',');
            } else if (c == '}' && INCON(L_EBND)) {
                INTOCON(L_ERE);
                RETV('}', 1);
            }
            // Handle other bound cases...
            break;

        case L_BRACK:  // Bracket expressions
            if (c == ']' && !LASTTYPE('[')) {
                INTOCON((v->cflags & REG_EXTENDED) ? L_ERE : L_BRE);
                RET(']');
            } else if (c == '\\' && (v->cflags & REG_ADVF)) {
                if (!lexescape(v)) return 0;
                // Handle escape result...
            } else if (c == '-') {
                RETV(LASTTYPE('[') || NEXT1(']') ? PLAIN : RANGE, c);
            }
            // Handle other bracket cases...
            break;
    }

    // Handle ERE/ARE context (main regex operators)
    if (INCON(L_ERE)) {
        switch (c) {
            case '|': RET('|');
            case '*':
                if ((v->cflags & REG_ADVF) && NEXT1('?')) {
                    v->now++; RETV('*', 0);  // Non-greedy
                }
                RETV('*', 1);
            case '+': case '?':
                // Similar non-greedy handling...
                RETV(c, 1);
            case '{':
                if (ATEOS() || !iscdigit(*v->now)) {
                    RETV(PLAIN, c);
                } else {
                    INTOCON(L_EBND);
                    RET('{');
                }
            case '(':
                // Handle advanced extensions (?:...), (?=...), etc.
                RETV('(', 1);
            case '[':
                INTOCON(L_BRACK);
                RETV('[', NEXT1('^') ? (v->now++, 0) : 1);
            case '.': case '^': case '$':
                RET(c);
            case '\\':
                if (ATEOS()) FAILW(REG_EESCAPE);
                return lexescape(v);
            default:
                RETV(PLAIN, c);
        }
    }

    return 1;
}
```