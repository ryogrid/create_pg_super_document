# pg_reg_colorisbegin

## Location
[src/backend/regex/regexport.c:191-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L191-L207)

## Overview
Checks whether a given color represents a beginning-of-line or beginning-of-string pseudo-character in a compiled regular expression.

## Definition


## Detailed Description
This function determines if a specified color code corresponds to a beginning-of-line (BOL) or beginning-of-string (BOS) pseudo-character in the regex engine. These are special "colors" that don't represent actual characters but instead represent positional assertions at the start of text.

The function examines the compiled NFA's  array, which contains two color values: one for beginning-of-string (BOS) and one for beginning-of-line (BOL). If the provided color matches either of these special colors, the function returns true.

## Parameters / Member Variables
- : Pointer to the compiled regular expression structure
- : Color number to check for beginning-of-line/string property

## Dependencies
- Functions called/Symbols referenced:
  - : Magic number constant for regex validation
  - : Internal regex structure containing the compiled NFA
  - : Compiled NFA structure containing positional color assignments
- Called from (representative examples):
  - External code that needs to distinguish positional pseudo-colors from regular character colors

## Notes and Other Information
- BOS (Beginning of String) matches only at the very start of the input text
- BOL (Beginning of Line) matches at the start of the input text or after any newline character
- These are "pseudo-colors" that represent positions rather than actual characters
- The function provides a simple binary check and may be extended in the future for more refined handling of pseudo-colors
- Returns true (non-zero) if the color is a beginning pseudo-color, false (0) otherwise