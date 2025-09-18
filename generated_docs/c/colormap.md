# colormap

## Location
src/include/regex/regguts.h: 228 - 230

## Overview
A comprehensive structure that manages character-to-color mapping in PostgreSQL's regex engine, providing efficient mapping for both low and high-valued character codes.

## Definition
```c
struct colormap
{
    int         magic;
#define CMMAGIC 0x876
    struct vars *v;                 /* for compile error reporting */
    size_t      ncds;               /* allocated length of colordescs array */
    size_t      max;                /* highest color number currently in use */
    color       free;               /* beginning of free chain (if non-0) */
    struct colordesc *cd;           /* pointer to array of colordescs */
#define CDEND(cm)   (&(cm)->cd[(cm)->max + 1])

    /* mapping data for chrs <= MAX_SIMPLE_CHR: */
    color      *locolormap;         /* simple array indexed by chr code */

    /* mapping data for chrs > MAX_SIMPLE_CHR: */
    int         classbits[NUM_CCLASSES];    /* see comment above */
    int         numcmranges;        /* number of colormapranges */
    colormaprange *cmranges;        /* ranges of high chrs */
    color      *hicolormap;         /* 2-D array of color entries */
    int         maxarrayrows;       /* number of array rows allocated */
    int         hiarrayrows;        /* number of array rows in use */
    int         hiarraycols;        /* number of array columns (2^N) */

    /* If we need up to NINLINECDS, we store them here to save a malloc */
#define NINLINECDS  ((size_t) 10)
    struct colordesc cdspace[NINLINECDS];
};
```

## Detailed Description
The `colormap` structure is the central data structure for character-to-color mapping in PostgreSQL's regex implementation. It implements a sophisticated two-tier approach to handle both common characters efficiently and large character sets without excessive memory usage.

For characters with codes up to MAX_SIMPLE_CHR, it uses a simple direct-indexed array (`locolormap`) for O(1) lookup. For higher character values, it employs a more complex 2-D array system (`hicolormap`) combined with character ranges (`cmranges`) and character class information (`classbits`) to maintain efficiency while supporting Unicode and large character sets.

The structure also manages color allocation through a free list mechanism and provides space for color descriptors either inline (for small numbers) or through dynamic allocation.

## Parameters / Member Variables
- `magic`: Magic number (CMMAGIC = 0x876) for structure validation
- `v`: Pointer to vars structure for compile error reporting
- `ncds`: Allocated length of the colordescs array
- `max`: Highest color number currently in use
- `free`: Beginning of free color chain (0 if none)
- `cd`: Pointer to array of color descriptors
- `locolormap`: Simple array for characters <= MAX_SIMPLE_CHR, indexed by character code
- `classbits[NUM_CCLASSES]`: Bit masks for locale-dependent character classes
- `numcmranges`: Number of colormaprange entries
- `cmranges`: Array of character ranges for high-valued characters
- `hicolormap`: 2-D array for mapping high-valued characters to colors
- `maxarrayrows`: Number of allocated rows in hicolormap
- `hiarrayrows`: Number of rows currently in use in hicolormap
- `hiarraycols`: Number of columns in hicolormap (always 2^N)
- `cdspace[NINLINECDS]`: Inline storage for small numbers of color descriptors

## Dependencies
- Functions called/Symbols referenced:
  - [vars](../v/vars.md) (for error reporting)
  - colordesc (color descriptor structure)
  - [colormaprange](colormaprange.md) (character range structure)
  - color (color type)

- Called from (representative examples):
  - [initcm](../i/initcm.md) (initialization)
  - [freecm](../f/freecm.md) (cleanup)
  - [pg_reg_getcolor](../p/pg_reg_getcolor.md) (color lookup)
  - [newcolor](../n/newcolor.md) (color allocation)
  - [subcolor](../s/subcolor.md) (color operations)
  - Various regex compilation and execution functions

## Notes and Other Information
- The GETCOLOR macro provides the primary interface for color lookup: direct array access for low characters, function call for high characters
- The CDEND macro provides a convenient way to get a pointer past the end of the color descriptor array
- The structure includes inline storage (cdspace) to avoid malloc overhead for small regex patterns
- The magic number is used for debugging and validation purposes
- This is a core component of PostgreSQL's regex engine optimization for handling Unicode efficiently
- The two-tier approach allows handling of both ASCII and Unicode characters without sacrificing performance for common cases