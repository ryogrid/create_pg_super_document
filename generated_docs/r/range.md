# range

## Location
[src/backend/regex/regc_locale.c:412-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_locale.c#L412-L487)

## Overview
The range function creates a character vector (cvec) representing a character range with optional case-independence support, including legality validation for PostgreSQL's regular expression engine.

## Definition


## Detailed Description
The range function is a core component of PostgreSQL's regex bracket expression processing that creates character vectors representing character ranges. It handles both simple case-sensitive ranges and complex case-independent ranges with sophisticated case folding.

The function operates in two distinct modes:

1. **Case-sensitive mode (cases=0)**: Creates a simple range from character a to character b using addrange() for efficient representation.

2. **Case-independent mode (cases=1)**: Creates a more complex character set that includes the original range plus all case equivalents. For characters whose case variants fall outside the original range, individual characters are added using addchr().

The function includes important safety measures:
- Range validity checking using the before() function
- Allocation limits (max 100,000 characters) to prevent excessive memory usage
- Overflow protection with REG_ETOOBIG error reporting
- Interrupt checking for long-running operations

## Parameters / Member Variables
- : Context structure containing regex compilation state and configuration
- : Starting character of the range (inclusive)
- : Ending character of the range (inclusive, may equal a for single character)
- : Flag indicating whether case-independent matching is required (non-zero for case-independent)

## Dependencies
- Functions called/Symbols referenced:
  - before (character ordering comparison)
  - getcvec (character vector allocation)
  - addrange (add character range to cvec)
  - addchr (add individual character to cvec)
  - pg_wc_tolower (lowercase conversion)
  - pg_wc_toupper (uppercase conversion)
  - NOERR (error checking macro)
  - ERR (error reporting macro)
  - INTERRUPT (check for query interruption)
  - REG_ERANGE (invalid range error)
  - REG_ETOOBIG (range too large error)
- Called from (representative examples):
  - [brackpart](../b/brackpart.md) (in regcomp.c:1874 for bracket expression ranges)

## Notes and Other Information
- Returns NULL on error conditions (invalid range or memory allocation failure)
- Case-independent processing adds case variants as individual characters rather than ranges for complexity management
- Implements protection against very large character ranges that could cause memory exhaustion
- Part of PostgreSQL's locale-aware regex processing system
- Uses PostgreSQL's character conversion functions for proper Unicode case handling