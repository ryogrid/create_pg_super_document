# prefixes

## Location
src/backend/regex/regc_lex.c: 99 - 199

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