# newhicolorcols

## Location
src/backend/regex/regc_color.c: 469 - 521

## Overview
Extends the hicolormap 2D array horizontally by duplicating existing columns, effectively doubling the width of the color mapping table.

## Definition


## Detailed Description
The  function is responsible for expanding the hicolormap array horizontally. It creates a new set of columns by copying the existing columns to the right, essentially doubling the width of the 2D color mapping array. The function performs in-place reallocation and works backwards through the rows to avoid overwriting data during the duplication process. After copying, it updates the reference counts for all colors to maintain proper bookkeeping.

## Parameters / Member Variables
- : Pointer to the colormap structure containing the hicolormap array and related metadata

## Dependencies
- Functions called/Symbols referenced:
  - CERR (error reporting macro)
  - REALLOC (memory reallocation macro)
  - REG_ESPACE (error code constant)
- Called from (representative examples):
  - subcolorcvec (at line 587)

## Notes and Other Information
- Does not return a value (void function)
- Doubles the  value after successful expansion
- Includes overflow protection by checking against INT_MAX before allocation
- Uses backwards iteration through rows to safely duplicate data in-place after reallocation
- Increases color reference counts (nuchrs) for all duplicated color entries
- Part of the regex engine's color compression system that manages efficient color-to-character mappings