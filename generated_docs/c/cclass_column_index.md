# cclass_column_index

## Location
src/backend/regex/regc_locale.c: 671 - 716

## Overview
The cclass_column_index function computes a column index for the high colormap based on which character classes a given character belongs to, used in PostgreSQL's regex color mapping system.

## Definition


## Detailed Description
The cclass_column_index function is a critical component of PostgreSQL's regex colormap optimization system. It determines the appropriate column index within the high-resolution colormap array by evaluating which character classes the given character belongs to.

The function works by:

1. **Character class evaluation**: It tests the character against all locale-dependent character classes (those handled by pg_wc_* functions) that are marked as active in the colormap's classbits array.

2. **Bitwise combination**: For each character class the character belongs to, it ORs the corresponding bit value from classbits into the result, creating a unique index that represents the combination of character class memberships.

3. **Locale-specific optimization**: Only processes character classes that are locale-dependent (those using pg_wc_* functions), as indicated by the assertions that hard-wired classes (ASCII, BLANK, CNTRL, XDIGIT) have zero classbits.

This function enables the regex engine to efficiently map characters to colors based on their character class memberships, which is essential for regex optimization. Characters with identical class memberships can share the same color, reducing the complexity of the finite automaton.

The function is only called for characters above MAX_SIMPLE_CHR, as simpler characters are handled through direct lookup mechanisms.

## Parameters / Member Variables
- : Pointer to the colormap structure containing class bit assignments and colormap configuration
- : The character for which to compute the column index (must be > MAX_SIMPLE_CHR)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_wc_isprint](../p/pg_wc_isprint.md), pg_wc_isalnum, pg_wc_isalpha, pg_wc_isword (character classification)
  - [pg_wc_isdigit](../p/pg_wc_isdigit.md), pg_wc_ispunct, pg_wc_isspace (character classification)  
  - [pg_wc_islower](../p/pg_wc_islower.md), pg_wc_isupper, pg_wc_isgraph (character classification)
  - CC_* constants (character class identifiers)
  - MAX_SIMPLE_CHR (threshold for simple character handling)
- Called from (representative examples):
  - [pg_reg_getcolor](../p/pg_reg_getcolor.md) (in regc_color.c:158 for colormap lookup)

## Notes and Other Information
- Only processes locale-dependent character classes; hard-wired classes are asserted to have zero classbits
- Returns a bitwise combination of active character class memberships as the column index
- Part of PostgreSQL's regex colormap optimization that groups characters by class membership
- Must be kept synchronized with the cclasscvec() function's character class handling
- Input character must be above MAX_SIMPLE_CHR threshold for this function to be called
- The returned index is used to access specific columns in the high-resolution colormap array