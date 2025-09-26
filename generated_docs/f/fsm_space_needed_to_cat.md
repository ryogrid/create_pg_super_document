# fsm_space_needed_to_cat

## Location
[src/backend/storage/freespace/freespace.c:432-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L432-L454)

## Overview
Determines which FSM (Free Space Map) category a page needs to have to accommodate a specified amount of data by rounding up the space requirement to the appropriate category.

## Definition
```c
static uint8 fsm_space_needed_to_cat(Size needed)
```

## Detailed Description
This function performs the inverse operation of fsm_space_avail_to_cat() by converting a space requirement (in bytes) to the minimum FSM category needed to satisfy that requirement. While fsm_space_avail_to_cat() rounds down from category to available space, this function rounds up from needed space to the required category.

The function uses ceiling division to ensure that any fractional space requirement is rounded up to the next category level. It includes validation to ensure the requested size doesn't exceed the maximum FSM request size and handles the special case where no space is needed (returns category 1).

## Parameters / Member Variables
- `needed`: The number of bytes of free space required on a page

## Dependencies
- Functions called/Symbols referenced:
  - MaxFSMRequestSize
  - FSM_CAT_STEP
- Called from (representative examples):
  - FSMAddress
  - GetPageWithFreeSpace
  - RecordAndGetPageWithFreeSpace

## Notes and Other Information
- The function implements ceiling division using the formula: (needed + FSM_CAT_STEP - 1) / FSM_CAT_STEP
- Returns category 1 (minimum) when no space is needed (needed == 0)
- Caps the maximum category at 255 (uint8 maximum value)
- Throws an ERROR if the requested size exceeds MaxFSMRequestSize
- This is a static function internal to the freespace.c module