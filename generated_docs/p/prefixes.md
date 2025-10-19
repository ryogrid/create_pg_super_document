# prefixes

## Location
[src/backend/regex/regc_lex.c:99-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_lex.c#L99-L199)

## Overview
Processes special prefix patterns and embedded options in regular expressions, setting appropriate compilation flags based on the recognized syntax patterns.

## Definition
```c
static void prefixes(struct vars *v)
```

## Detailed Description
The `prefixes` function implements various special prefix handling for regular expression compilation. It processes two main types of prefixes:

1. **Triple-star prefixes (`***`)**: Special sequences that modify compilation behavior:
   - `***?`: Error pattern showing version information
   - `***=`: Switches to literal string mode (REG_QUOTE)
   - `***:`: Switches to Advanced Regular Expression (ARE) mode

2. **Embedded options (`(?...)`)**: Available only in ARE mode, allows runtime modification of regex behavior through single-character flags:
   - `b`: Switch to Basic Regular Expression mode
   - `c`: Case sensitive matching
   - `e`: Plain Extended Regular Expression mode
   - `i`: Case insensitive matching
   - `m`/`n`: Enable newline-sensitive matching
   - `p`: Perl-like newline handling
   - `q`: Literal string mode
   - `s`: Single line mode (newline treated as ordinary character)
   - `t`: Tight syntax (disable expanded mode)
   - `w`: Weird newline mode (affects only ^ and $)
   - `x`: Expanded syntax (allow whitespace and comments)

The function modifies the compilation flags (cflags) in the vars structure based on the recognized patterns, and advances the input pointer past processed prefixes.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing:
  - `cflags`: Compilation flags that control regex behavior
  - `now`: Current position in the input string being processed

## Dependencies
- Functions called/Symbols referenced:
  - HAVE, NEXT1, NEXT2, NEXT3, ATEOS
  - CHR, ERR, NOTE
  - iscalpha
- Constants referenced:
  - REG_QUOTE, REG_ADVANCED, REG_EXTENDED, REG_EXPANDED, REG_NEWLINE
  - REG_ICASE, REG_NLSTOP, REG_NLANCH, REG_ADVF
  - REG_BADPAT, REG_BADRPT, REG_BADOPT, REG_UNONPOSIX
- Called from (representative examples):
  - [lexstart](../l/lexstart.md) (in regc_lex.c)

## Notes and Other Information
The function returns early if REG_QUOTE is already set, as literal strings do not support prefix processing. Embedded options are only available in Advanced Regular Expression mode, providing fine-grained control over regex behavior. The function includes error handling for malformed prefix patterns and unsupported option combinations. Non-POSIX features trigger REG_UNONPOSIX notifications for compliance tracking.

## Simplified Source

```c
static void prefixes(struct vars *v) {
    // Skip prefix processing for literal strings
    if (v->cflags & REG_QUOTE)
        return;

    // Handle "***" special prefixes
    if (HAVE(4) && NEXT3('*', '*', '*')) {
        switch (*(v->now + 3)) {
            case CHR('?'):  // Error pattern
                ERR(REG_BADPAT);
                return;
            case CHR('='):  // Switch to literal string mode
                NOTE(REG_UNONPOSIX);
                v->cflags |= REG_QUOTE;
                v->cflags &= ~(REG_ADVANCED | REG_EXPANDED | REG_NEWLINE);
                v->now += 4;
                return;
            case CHR(':'):  // Switch to ARE mode
                NOTE(REG_UNONPOSIX);
                v->cflags |= REG_ADVANCED;
                v->now += 4;
                break;
            default:
                ERR(REG_BADRPT);
                return;
        }
    }

    // Skip embedded options for non-advanced modes
    if ((v->cflags & REG_ADVANCED) != REG_ADVANCED)
        return;

    // Process embedded options "(?...)" for AREs
    if (HAVE(3) && NEXT2('(', '?') && iscalpha(*(v->now + 2))) {
        NOTE(REG_UNONPOSIX);
        v->now += 2;

        // Process option characters
        for (; !ATEOS() && iscalpha(*v->now); v->now++) {
            switch (*v->now) {
                case CHR('i'):  // Case insensitive
                    v->cflags |= REG_ICASE;
                    break;
                case CHR('c'):  // Case sensitive
                    v->cflags &= ~REG_ICASE;
                    break;
                case CHR('n'):  // Newline sensitive
                    v->cflags |= REG_NEWLINE;
                    break;
                case CHR('s'):  // Single line mode
                    v->cflags &= ~REG_NEWLINE;
                    break;
                case CHR('x'):  // Expanded syntax
                    v->cflags |= REG_EXPANDED;
                    break;
                case CHR('q'):  // Literal string
                    v->cflags |= REG_QUOTE;
                    v->cflags &= ~REG_ADVANCED;
                    break;
                // Additional cases simplified for brevity
                default:
                    ERR(REG_BADOPT);
                    return;
            }
        }

        // Require closing parenthesis
        if (!NEXT1(')')) {
            ERR(REG_BADOPT);
            return;
        }
        v->now++;
    }
}
```