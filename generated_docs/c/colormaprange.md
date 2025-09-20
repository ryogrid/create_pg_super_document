# colormaprange

## Location
[src/include/regex/regguts.h:221-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L221-L226)

## Overview
A structure representing a character range in PostgreSQL's regex implementation that maps high-valued character codes to row indexes in the hicolormap array for efficient color mapping.

## Definition

```c
typedef struct colormaprange
{
	chr			cmin;			/* range represents cmin..cmax inclusive */
	chr			cmax;
	int			rownum;			/* row index in hicolormap array (>= 1) */
} colormaprange;
```
## Detailed Description
The  structure is a key component of PostgreSQL's regex color mapping system for handling large character sets efficiently. Instead of using a simple array indexed by character codes (which would be impractical for large character sets), PostgreSQL uses a two-tier approach: simple arrays for common characters up to MAX_SIMPLE_CHR, and a more complex 2-D array system for higher character values.

Each  represents a contiguous range of character codes (from  to  inclusive) that have been specifically mentioned in the regex pattern. These ranges are used to determine the row index in the hicolormap array, which along with character class information (column index), allows efficient lookup of colors for high-valued characters.

The colormapranges must be:
- Nonempty (cmin <= cmax)
- Nonoverlapping 
- Ordered by increasing character values

## Parameters / Member Variables
- `cmin`: The minimum character code in this range (inclusive)
- `cmax`: The maximum character code in this range (inclusive)
- `rownum`: The row index in the hicolormap array (must be >= 1, with row 0 reserved for characters not in any specific range)
## Dependencies
- Functions called/Symbols referenced:
  - [chr](chr.md) (character type)

- Called from (representative examples):
  - [pg_reg_getcolor](../p/pg_reg_getcolor.md)
  - [subcoloronechr](../s/subcoloronechr.md)
  - [subcoloronerange](../s/subcoloronerange.md)
  - [CDEND](../C/CDEND.md)

## Notes and Other Information
- Part of PostgreSQL's regex engine optimization for handling Unicode and large character sets
- Works in conjunction with the hicolormap 2-D array and character class bit masks
- The structure is defined in regguts.h, which contains internal definitions for the regex implementation
- Row 0 in the hicolormap is implicitly used for characters that don't fall into any defined colormaprange
- This design allows the regex engine to efficiently handle both common ASCII characters and rare Unicode characters without excessive memory usage