# lexescape

## Location
[src/backend/regex/regc_lex.c:601-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_lex.c#L601-L779)

## Overview
Parses Advanced Regular Expression (ARE) backslash escape sequences after the backslash has already been consumed, converting them into appropriate tokens for the regex parser.

## Definition
```c
static int lexescape(struct vars *v)
```

## Detailed Description
The `lexescape` function handles the interpretation of backslash escape sequences in Advanced Regular Expressions. It processes the character following a backslash and converts it into the appropriate token type and value. The function supports a comprehensive set of escape sequences including:

**Character escapes:**
- Standard C escapes: `\a` (alert), `\b` (backspace), `\f` (form feed), `\n` (newline), `\r` (carriage return), `\t` (tab), `\v` (vertical tab)
- Unicode escapes: `\u` (4-digit hex), `\U` (8-digit hex), `\x` (hex)
- Control characters: `\c` (control character)
- Octal escapes: `\0` through `\377`

**Character class shortcuts:**
- `\d`/`\D`: Digit/non-digit characters
- `\s`/`\S`: Space/non-space characters  
- `\w`/`\W`: Word/non-word characters

**Anchors and boundaries:**
- `\A`: Start of string anchor (SBEGIN)
- `\Z`: End of string anchor (SEND)
- `\m`/`\<`: Word boundary start
- `\M`/`\>`: Word boundary end
- `\y`: Word boundary (WBDRY)
- `\Y`: Non-word boundary (NWBDRY)

**Backreferences:**
- `\1` through `\9`: Backreferences to captured groups

The function includes heuristics to distinguish between backreferences and octal escapes, and provides comprehensive error handling for malformed escape sequences.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing:
  - `cflags`: Must have REG_ADVF flag set (Advanced Regular Expression mode)
  - `now`: Current position in input string (backslash already consumed)
  - `nsubexp`: Number of subexpressions for backref validation

## Dependencies
- Functions called/Symbols referenced:
  - [lexdigits](lexdigits.md), chrnamed
  - ATEOS, ISERR, CHR_IS_IN_RANGE, ENDOF
  - RETV, RET, FAILW, NOTE, ERR
  - CHR, DIGITVAL
- Constants referenced:
  - Token types: PLAIN, SBEGIN, SEND, CCLASSS, CCLASSC, WBDRY, NWBDRY, BACKREF
  - Character classes: CC_DIGIT, CC_SPACE, CC_WORD
  - Error/notification codes: REG_EESCAPE, REG_UNONPOSIX, REG_UUNPORT, REG_ULOCALE, REG_UBACKREF
- Called from (representative examples):
  - [next](../n/next.md) (for general backslash processing)
  - [next](../n/next.md) (within bracket expressions)

## Notes and Other Information
This function is only used in Advanced Regular Expression mode (REG_ADVF flag must be set). It can be called from both normal contexts and bracket expressions, though the caller must validate that certain escape types are appropriate for the context. The function includes extensive use of NOTE() calls to track non-POSIX features for compliance reporting. Unrecognized alphabetic escape sequences generate errors to reserve them for future use. The backref vs. octal disambiguation uses heuristics based on the number of digits and whether the number is within the valid subexpression range.

## Simplified Source

```c
static int
lexescape(struct vars *v)
{
    chr c;
    const chr *save;

    assert(v->cflags & REG_ADVF);
    assert(!ATEOS());

    c = *v->now++;

    // Non-alphanumeric ASCII characters are treated as plain characters
    if (!('a' <= c && c <= 'z') && !('A' <= c && c <= 'Z') && !('0' <= c && c <= '9')) {
        RETV(PLAIN, c);
    }

    NOTE(REG_UNONPOSIX);

    switch (c) {
        // Standard character escapes
        case 'a': RETV(PLAIN, CHR('\007'));     // alert/bell
        case 'b': RETV(PLAIN, CHR('\b'));       // backspace
        case 'f': RETV(PLAIN, CHR('\f'));       // form feed
        case 'n': RETV(PLAIN, CHR('\n'));       // newline
        case 'r': RETV(PLAIN, CHR('\r'));       // carriage return
        case 't': RETV(PLAIN, CHR('\t'));       // tab
        case 'v': RETV(PLAIN, CHR('\v'));       // vertical tab
        case 'B': RETV(PLAIN, CHR('\\'));       // backslash

        // Control character
        case 'c':
            if (ATEOS()) FAILW(REG_EESCAPE);
            RETV(PLAIN, (chr) (*v->now++ & 037));

        // Character class shortcuts
        case 'd': RETV(CCLASSS, CC_DIGIT);      // digits
        case 'D': RETV(CCLASSC, CC_DIGIT);      // non-digits
        case 's': RETV(CCLASSS, CC_SPACE);      // whitespace
        case 'S': RETV(CCLASSC, CC_SPACE);      // non-whitespace
        case 'w': RETV(CCLASSS, CC_WORD);       // word characters
        case 'W': RETV(CCLASSC, CC_WORD);       // non-word characters

        // Anchors and boundaries
        case 'A': RETV(SBEGIN, 0);              // start of string
        case 'Z': RETV(SEND, 0);                // end of string
        case 'm': RET('<');                     // word boundary start
        case 'M': RET('>');                     // word boundary end
        case 'y': RETV(WBDRY, 0);               // word boundary
        case 'Y': RETV(NWBDRY, 0);              // non-word boundary

        // Unicode escapes
        case 'u':
            c = lexdigits(v, 16, 4, 4);
            if (ISERR() || !CHR_IS_IN_RANGE(c)) FAILW(REG_EESCAPE);
            RETV(PLAIN, c);

        case 'U':
            c = lexdigits(v, 16, 8, 8);
            if (ISERR() || !CHR_IS_IN_RANGE(c)) FAILW(REG_EESCAPE);
            RETV(PLAIN, c);

        case 'x':
            c = lexdigits(v, 16, 1, 255);
            if (ISERR() || !CHR_IS_IN_RANGE(c)) FAILW(REG_EESCAPE);
            RETV(PLAIN, c);

        // Backreferences and octal
        case '1': case '2': case '3': case '4': case '5':
        case '6': case '7': case '8': case '9':
            save = v->now;
            v->now--;  // Put digit back
            c = lexdigits(v, 10, 1, 255);
            if (ISERR()) FAILW(REG_EESCAPE);

            // Check if it's a valid backreference
            if (v->now == save || ((int) c > 0 && (int) c <= v->nsubexp)) {
                RETV(BACKREF, c);
            }
            // Fall through to octal handling
            v->now = save;

        case '0':
            v->now--;  // Put digit back
            c = lexdigits(v, 8, 1, 3);
            if (ISERR()) FAILW(REG_EESCAPE);
            if (c > 0xff) {
                v->now--;
                c >>= 3;
            }
            RETV(PLAIN, c);

        default:
            // Unrecognized escape - reserved for future use
            FAILW(REG_EESCAPE);
    }

    assert(NOTREACHED);
}
```