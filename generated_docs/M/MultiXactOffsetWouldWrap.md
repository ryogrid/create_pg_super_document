# MultiXactOffsetWouldWrap

## Location
[src/backend/access/transam/multixact.c:2832-2879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2832-L2879)

## Overview
Determines whether adding a distance to a start offset would move past a boundary point, accounting for wraparound in the full 32-bit unsigned integer space.

## Definition

```c
static bool
MultiXactOffsetWouldWrap(MultiXactOffset boundary, MultiXactOffset start,
						 uint32 distance)
```
## Detailed Description
This function determines whether adding a specified distance to a starting offset would cross a boundary point, taking into account wraparound behavior in the full 2^32-1 space. Unlike regular 2^31-modulo arithmetic, this function is designed to utilize the entire 32-bit unsigned integer space for multixact offsets. It handles the special case where offset 0 is not used (as noted in GetMultiXactIdMembers) by incrementing the finish value when it wraps around UINT_MAX.

The function implements different logic depending on whether the boundary is numerically greater or less than the starting point, handling both normal progression and UINT_MAX wraparound scenarios correctly.

## Parameters / Member Variables
- : The boundary offset that should not be crossed
- : The starting offset from which to measure
- : The distance to add to the start offset

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactOffset (type definitions)
  - uint32 (standard type)
- Called from (representative examples):
  - debug_elog6 (src/backend/access/transam/multixact.c:411)
  - OFFSET_WARN_SEGMENTS (src/backend/access/transam/multixact.c:1181, 1221)

## Notes and Other Information
- Returns true if the addition would wrap past the boundary, false otherwise
- Handles UINT_MAX wraparound by skipping offset 0 (which is not used)
- Uses different logic for boundary < start vs boundary >= start cases
- Enables use of the full 32-bit unsigned integer space for multixact offsets
- Critical for preventing data corruption in MultiXact member offset management
- Function is located at src/backend/access/transam/multixact.c:2832-2879