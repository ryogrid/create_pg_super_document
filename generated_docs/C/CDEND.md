# CDEND

## Location
src/include/regex/regguts.h: 237 - 251

## Overview
A macro that provides a pointer to one position past the end of the color descriptor array in a colormap structure, used for iteration and boundary checking.

## Definition
```c
#define CDEND(cm)   (&(cm)->cd[(cm)->max + 1])
```

## Detailed Description
The `CDEND` macro is a utility macro that calculates a pointer to the position immediately following the last valid color descriptor in a colormap's color descriptor array. It takes a colormap pointer (`cm`) and returns a pointer that can be used as an end sentinel for iteration over the color descriptors.

The macro works by taking the base address of the color descriptor array (`cd`) and adding an offset equal to the maximum color number plus one (`max + 1`). Since color numbers start from 0, this gives a pointer to the first invalid position after all valid color descriptors.

This follows the common C idiom of using one-past-the-end pointers for range iteration, similar to how standard library iterators work.

## Parameters / Member Variables
- `cm`: Pointer to a colormap structure containing the color descriptor array

## Dependencies
- Functions called/Symbols referenced:
  - color (color type used in array indexing)
  - NUM_CCLASSES (referenced in the broader context)
  - [colormaprange](../c/colormaprange.md) (referenced in the broader context)

- Called from (representative examples):
  - [okcolors](../o/okcolors.md) (color validation)
  - [rainbow](../r/rainbow.md) (color assignment)
  - [colorcomplement](../c/colorcomplement.md) (color complementing)
  - [dumpcolors](../d/dumpcolors.md) (debugging output)
  - optimizebracket (bracket optimization)

## Notes and Other Information
- This is a common C programming pattern for providing end-of-array sentinels
- Used primarily in loops that iterate over all color descriptors in a colormap
- The macro assumes that the colormap structure is properly initialized with valid `cd` and `max` fields
- Part of the internal API for PostgreSQL's regex color management system
- Provides type safety by returning a properly typed pointer to colordesc structures
- Essential for safe iteration without buffer overruns in color processing algorithms