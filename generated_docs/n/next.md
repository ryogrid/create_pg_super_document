# next

## Location
src/backend/regex/regc_lex.c: 200 - 600

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