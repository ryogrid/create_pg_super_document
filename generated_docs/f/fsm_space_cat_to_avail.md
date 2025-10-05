# fsm_space_cat_to_avail

## Location
[src/backend/storage/freespace/freespace.c:418-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L418-L431)

## Overview
fsm_space_cat_to_avail converts a Free Space Map category code (0-255) back to the lower bound of the byte range it represents, serving as the inverse operation of fsm_space_avail_to_cat.

## Definition
static Size fsm_space_cat_to_avail(uint8 cat)

## Detailed Description
This internal function performs the inverse mapping of fsm_space_avail_to_cat, converting FSM category codes back to byte counts of available free space. Since the categorization process involves lossy compression, this function returns the lower bound of the range that the category represents, not the exact original value.

The function implements the following reverse mapping:
- Category 255 (the highest category) returns exactly MaxFSMRequestSize bytes
- All other categories (0-254) return cat * FSM_CAT_STEP bytes

This provides a conservative estimate of available free space, ensuring that allocation requests based on these values will not fail due to insufficient space. The returned value represents the minimum guaranteed free space for the given category.

## Parameters / Member Variables
- : The FSM category code (0-255) to convert to a byte count

## Dependencies
- Functions called/Symbols referenced:
  - MaxFSMRequestSize (maximum free space request size constant)
  - FSM_CAT_STEP (step size for category calculation)
- Called from (representative examples):
  - [GetRecordedFreeSpace](../G/GetRecordedFreeSpace.md) (src/backend/storage/freespace/freespace.c:260)

## Notes and Other Information
- This is a static internal function, not exposed in the public API
- Returns the lower bound of the free space range represented by the category
- Category 255 is a special case that returns exactly MaxFSMRequestSize
- The function is the inverse of fsm_space_avail_to_cat but cannot restore exact original values due to lossy compression
- Essential for translating stored FSM category information back to usable byte counts
- Provides conservative estimates to ensure allocation requests succeed
- Located in src/backend/storage/freespace/freespace.c:413-425

## Simplified Source

```c
static Size fsm_space_cat_to_avail(uint8 cat) {
    // Special case: highest category represents exact MaxFSMRequestSize
    if (cat == 255)
        return MaxFSMRequestSize;

    // All other categories: multiply by step size
    return cat * FSM_CAT_STEP;
}
```