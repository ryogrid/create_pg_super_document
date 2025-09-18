# lexescape

## Location
src/backend/regex/regc_lex.c: 601 - 779

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