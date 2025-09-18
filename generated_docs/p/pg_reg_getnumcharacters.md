# pg_reg_getnumcharacters

## Location
[src/backend/regex/regexport.c:230-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L230-L265)

## Overview
Returns the number of characters that belong to a specified color (character equivalence class) in a compiled regular expression's colormap.

## Definition


## Detailed Description
This function determines how many individual characters are members of a given color in the regex engine's character classification system. Colors represent equivalence classes of characters that are treated identically by the regular expression.

The function examines the colordesc structure for the specified color and returns the count of simple characters (). However, it returns -1 in several cases:
- Invalid color numbers (≤ 0 or > max color)
- Special colors (WHITE, RAINBOW)  
- Pseudocolors (positional assertions like BOS, EOS)
- Colors with uncertain membership (those appearing in the high colormap)

The function specifically checks if the color has entries in the high colormap (), which would make the exact character count expensive to compute and potentially uncertain.

## Parameters / Member Variables
- : Pointer to the compiled regular expression structure
- : Color number to query for character count

## Dependencies
- Functions called/Symbols referenced:
  - : Magic number constant for regex validation
  - : Internal regex structure containing the colormap
  - : Structure that maps characters to color equivalence classes
  - : Flag bit indicating pseudocolors (positional assertions)
- Called from (representative examples):
  - External code that needs to analyze character class sizes in regex patterns

## Notes and Other Information
- Returns -1 for invalid, special, pseudo, or uncertain colors
- Only returns positive counts for regular colors with known simple character membership
- Simple characters have codes ≤ MAX_SIMPLE_CHR and use direct array indexing
- Complex characters (high Unicode) use a more sophisticated mapping that makes exact counts expensive
- The function prioritizes performance by avoiding expensive character enumeration for complex cases
- Colors with  indicate presence in the high colormap, making membership uncertain
- Used to determine if a color class is worth analyzing in detail during regex processing