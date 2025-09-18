# newcolor

## Location
[src/backend/regex/regc_color.c:185-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L185-L256)

## Overview
Allocates and initializes a new color descriptor in the colormap, with dynamic memory management for color descriptor array expansion.

## Definition
```c
static color newcolor(struct colormap *cm)
```

## Detailed Description
The `newcolor` function manages the allocation of new color identifiers within a colormap structure. It implements a sophisticated allocation strategy with three phases:

1. **Free list reuse**: First attempts to reuse a previously freed color from the free list (linked via the `sub` field)
2. **Sequential allocation**: If no free colors exist, allocates the next sequential color up to the current array size
3. **Array expansion**: When the array is full, doubles the size (up to MAX_COLOR limit) and handles the transition from inline storage to dynamic allocation

The function includes comprehensive error handling for memory allocation failures and color limit exhaustion. It properly initializes new color descriptors with default values and maintains the integrity of the colormap structure.

## Parameters / Member Variables
- `cm`: Pointer to colormap structure where the new color will be allocated

## Dependencies
- Functions called/Symbols referenced:
  - CISERR (error checking macro)
  - UNUSEDCOLOR (macro to check if color descriptor is unused)
  - MALLOC/REALLOC (memory allocation functions)
  - CERR (error reporting macro)
  - memcpy (memory copying)
- Called from (representative examples):
  - [pseudocolor](../p/pseudocolor.md) (in regc_color.c)
  - [newsub](newsub.md) (in regc_color.c)

## Notes and Other Information
- Returns COLORLESS on error conditions (memory allocation failure or color limit exceeded)
- May relocate the color descriptor array, potentially invalidating existing pointers
- Handles transition from inline storage (cdspace) to dynamically allocated storage seamlessly
- Uses a free list mechanism to efficiently reuse previously freed colors
- Enforces the MAX_COLOR limit to prevent excessive memory usage
- Critical function for color assignment during regular expression compilation
- All new color descriptors are initialized with safe default values