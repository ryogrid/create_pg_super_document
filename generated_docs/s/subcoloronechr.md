# subcoloronechr

## Location
src/backend/regex/regc_color.c: 624 - 746

## Overview
Handles subcolor allocation for a single character, creating NFA arcs and managing colormap ranges by splitting or creating ranges as needed.

## Definition


## Detailed Description
The  function is a specialized version of subcolor processing that handles individual characters efficiently. For simple characters (≤ MAX_SIMPLE_CHR), it directly calls . For complex characters, it manages the colormap ranges by potentially splitting existing ranges to isolate the target character. The function creates new colormaprange structures as needed, potentially splitting a single range into up to three parts: before the target character, the target character itself, and after the target character. It maintains the hicolormap structure by cloning rows when ranges are split.

## Parameters / Member Variables
- : Pointer to the regex compilation variables structure
- : The character to process for subcolor allocation
- : Pointer to the source state for NFA arcs
- : Pointer to the destination state for NFA arcs  
- : Pointer to the last subcolor created (for optimization to avoid duplicate arcs)

## Dependencies
- Functions called/Symbols referenced:
  - [subcolor](subcolor.md) (gets subcolor for simple characters)
  - [newarc](../n/newarc.md) (creates NFA arcs)
  - [newhicolorrow](../n/newhicolorrow.md) (creates new rows in hicolormap)
  - [subcoloronerow](subcoloronerow.md) (processes a row for subcolor allocation)
  - MALLOC/FREE (memory allocation/deallocation)
  - NOERR/CERR (error checking/reporting macros)
- Called from (representative examples):
  - [subcolorcvec](subcolorcvec.md) (at lines 539, 571)
  - [onechr](../o/onechr.md) (at line 1920)

## Notes and Other Information
- Does not return a value (void function)
- Optimizes simple character processing by using direct subcolor lookup
- Can split existing colormap ranges into up to three new ranges when processing complex characters
- Updates the  parameter to avoid creating duplicate arcs
- Manages dynamic memory allocation for the new colormaprange array
- Uses assertion to verify space estimation was adequate
- Part of the regex engine's color management system that maintains efficient character-to-color mappings while preserving the ability to distinguish individual characters when needed