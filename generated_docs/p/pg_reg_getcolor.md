# pg_reg_getcolor

## Location
[src/backend/regex/regc_color.c:120-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L120-L171)

## Overview
Determines the color assignment for characters outside the simple character range using binary search and locale-dependent character classification.

## Definition
```c
color pg_reg_getcolor(struct colormap *cm, chr c)
```

## Detailed Description
The `pg_reg_getcolor` function is the "slow case" implementation of the GETCOLOR() macro, handling characters that fall outside the simple character range (above MAX_SIMPLE_CHR). It uses a two-stage lookup process:

1. **Row determination**: Uses binary search on the sorted colormapranges array to find which character range the input character belongs to, determining the row in the hicolormap
2. **Column determination**: If multiple character classes are active (hiarraycols > 1), calls cclass_column_index() to determine the appropriate column based on locale-dependent character classification

The function is optimized for the common case where no character classes are defined, providing a fast path that directly returns the color from the single-column hicolormap.

## Parameters / Member Variables
- `cm`: Pointer to colormap structure containing character-to-color mappings
- `c`: Character to look up (must be > MAX_SIMPLE_CHR)

## Dependencies
- Functions called/Symbols referenced:
  - [cclass_column_index](../c/cclass_column_index.md) (for locale-dependent character classification)
  - [colormaprange](../c/colormaprange.md) (structure for character range information)
  - assert (for debugging assertions)
- Called from (representative examples):
  - GETCOLOR (macro in regguts.h)

## Notes and Other Information
- Only handles characters above MAX_SIMPLE_CHR; simpler characters are handled by direct array lookup
- Uses binary search for efficient range lookup even with many character ranges
- The function handles both single-column (simple) and multi-column (character class aware) color maps
- Row 0 is used as the default when no matching character range is found
- Critical for Unicode support and locale-aware regular expression matching