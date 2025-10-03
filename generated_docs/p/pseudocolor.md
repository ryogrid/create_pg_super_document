# pseudocolor

## Location
[src/backend/regex/regc_color.c:312-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L312-L335)

## Overview
Allocates a false or pseudo color in the colormap that is intended to be managed by external means rather than through normal character-to-color mapping.

## Definition

```c
static color
pseudocolor(struct colormap *cm)
```
## Detailed Description
The pseudocolor function creates a special type of color called a "pseudo color" or "false color" that doesn't correspond to actual characters in the input. These colors are used internally by the regex engine for special purposes and are managed by other parts of the system rather than through the normal character mapping mechanisms.

The function allocates a new color using newcolor() and then configures it with special properties:
- Sets nuchrs to 1 to pretend it exists in the upper character map
- Sets nschrs to 0 since it doesn't map to single-byte characters
- Initializes other fields to safe default values
- Marks it with the PSEUDO flag to distinguish it from regular colors

## Parameters / Member Variables
- `*cm`: Pointer to the colormap structure where the pseudo color will be allocated
## Dependencies
- Functions called/Symbols referenced:
  - [newcolor](../n/newcolor.md) (allocates a new color)
  - CISERR (macro to check for compilation errors)
  - COLORLESS (constant representing no color/error state)
  - NOSUB (constant indicating no subcolor)
  - CHR_MIN (minimum character value)
  - PSEUDO (flag indicating a pseudo color)
- Called from (representative examples):
  - [specialcolors](../s/specialcolors.md) (at src/backend/regex/regc_nfa.c:1560-1563)

## Notes and Other Information
- Returns COLORLESS if an error occurs during color allocation
- Pseudo colors are used for special regex engine operations and don't represent actual character classes
- The function pretends the color is in the upper character map by setting nuchrs to 1
- These colors are typically used for things like word boundaries, line boundaries, and other special regex constructs
- Part of PostgreSQL's regular expression engine color management system