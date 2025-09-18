# ginCompareEntries

## Location
src/backend/access/gin/ginutil.c: 388 - 409

## Overview
Compares two GIN index keys of the same column, handling null categories and delegating to the appropriate comparison function for non-null values.

## Definition
```c
int ginCompareEntries(GinState *ginstate, OffsetNumber attnum,
                     Datum a, GinNullCategory categorya,
                     Datum b, GinNullCategory categoryb)
```

## Detailed Description
ginCompareEntries is a core comparison function in the GIN access method that provides a three-way comparison between two index keys from the same column. The function implements a hierarchical comparison strategy: it first compares null categories, then handles the special case where both values are non-normal (null or placeholder), and finally delegates to the column-specific comparison function for normal values.

This function is essential for maintaining the sorted order of GIN index entries and is used throughout GIN operations including scanning, searching, and building operations. The function handles the complex null semantics of GIN indexes where different types of null and placeholder values need to be distinguished and ordered consistently.

## Parameters / Member Variables
- `ginstate`: Pointer to GIN state structure containing comparison functions and collation information
- `attnum`: 1-based attribute number (column number) being compared
- `a`: First datum value to compare
- `categorya`: Null category classification for the first value
- `b`: Second datum value to compare  
- `categoryb`: Null category classification for the second value

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md): PostgreSQL function to extract int32 from Datum result
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md): PostgreSQL function to invoke a collation-aware comparison function
  - [GinState](../G/GinState.md): Structure containing GIN index state and comparison functions
  - GinNullCategory: Enumeration for categorizing null and special values
  - GIN_CAT_NORM_KEY: Constant representing normal (non-null) key values

- Called from (representative examples):
  - [collectMatchBitmap](../c/collectMatchBitmap.md): During index scanning and bitmap collection
  - [collectMatchesForHeapRow](../c/collectMatchesForHeapRow.md): When collecting matches for specific heap rows
  - [ginFillScanEntry](ginFillScanEntry.md): During scan entry preparation
  - [ginCompareAttEntries](ginCompareAttEntries.md): Higher-level comparison function for entries with attributes

## Notes and Other Information
- Returns negative, zero, or positive integer following standard comparison conventions
- Null category comparison takes precedence over value comparison
- All values within the same non-normal null category are considered equal
- For normal values, delegates to the column's specific comparison function with proper collation
- The function uses 1-based attribute numbering, requiring adjustment when accessing arrays
- Essential for maintaining B-tree ordering properties in GIN entry pages
- Handles the complex null semantics required for GIN's inverted index structure