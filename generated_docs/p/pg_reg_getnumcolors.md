# pg_reg_getnumcolors

## Location
src/backend/regex/regexport.c: 174 - 190

## Overview
Returns the total number of colors defined in a compiled regular expression's character colormap.

## Definition


## Detailed Description
This function retrieves the total number of colors from a compiled regular expression's colormap. In the regex engine, characters are grouped into "colors" - equivalence classes of characters that are treated identically by the regular expression. This optimization reduces the size of the NFA by grouping characters that have the same behavior in the regex.

The function accesses the colormap structure within the regex's internal guts and returns , where  is the highest color number currently in use. Since color numbering starts from 0, adding 1 gives the total count.

## Parameters / Member Variables
- : Pointer to the compiled regular expression structure containing the colormap

## Dependencies
- Functions called/Symbols referenced:
  - : Magic number constant for regex validation
  - : Internal regex structure containing the colormap
  - : Structure that maps characters to color equivalence classes
- Called from (representative examples):
  - External code that needs to understand the regex's character classification system

## Notes and Other Information
- Colors are equivalence classes that group characters with identical behavior in the regex
- The colormap optimization reduces NFA complexity by treating equivalent characters identically  
- Color numbers start from 0, so the total count is 
- The colormap contains both a simple array for characters ≤ MAX_SIMPLE_CHR and a more complex mapping system for higher Unicode characters
- The function performs basic validation by checking the regex magic number before accessing internal structures