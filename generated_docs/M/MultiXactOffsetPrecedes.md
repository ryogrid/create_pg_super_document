# MultiXactOffsetPrecedes

## Location
[src/backend/access/transam/multixact.c:3335-3346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3335-L3346)

## Overview
MultiXactOffsetPrecedes determines which of two MultiXact offset values is earlier, using wrap-around arithmetic for the offset space.

## Definition

```c
static bool
MultiXactOffsetPrecedes(MultiXactOffset offset1, MultiXactOffset offset2)
```
## Detailed Description
This function implements a precedence comparison for MultiXact offsets using modular arithmetic to handle wrap-around behavior. It computes the difference between the two offsets as a signed 32-bit integer and returns true if offset1 precedes offset2. The function follows the same pattern as MultiXactIdPrecedes but operates on the offset space rather than the MultiXact ID space.

MultiXact offsets are used to locate member information in the MultiXact member storage. Each MultiXact ID corresponds to an offset that points to the location where its member transaction IDs and lock modes are stored. This function is essential for determining the relative positioning of these offsets for cleanup and management operations.

## Parameters / Member Variables
- : First MultiXact offset to compare (MultiXactOffset)
- : Second MultiXact offset to compare (MultiXactOffset)

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactOffset (type)
- Called from (representative examples):
  - debug_elog6
  - [MultiXactAdvanceNextMXact](MultiXactAdvanceNextMXact.md)
  - [MultiXactMemberPagePrecedes](MultiXactMemberPagePrecedes.md)

## Notes and Other Information
- This is a static function internal to multixact.c
- Uses signed 32-bit arithmetic to handle wrap-around in the MultiXact offset space
- Returns true if offset1 is earlier (precedes) offset2
- The implementation assumes offsets being compared are within 2^31 of each other
- Critical for MultiXact member page management and cleanup operations
- Used by MultiXactMemberPagePrecedes to determine page precedence based on offset ranges
- Part of the internal infrastructure for managing MultiXact member storage

## Simplified Source

```c
// Simplified version of MultiXactOffsetPrecedes
static bool
MultiXactOffsetPrecedes(MultiXactOffset offset1, MultiXactOffset offset2)
{
    // Calculate signed difference to handle wrap-around arithmetic
    int32 diff = (int32) (offset1 - offset2);

    // Return true if offset1 is earlier than offset2
    return (diff < 0);
}
```

Key simplifications made:
- Added explanatory comments for the wrap-around arithmetic logic
- Clarified the purpose of the signed difference calculation
- Made the return condition more explicit with a comment
- The original function is already quite simple, so minimal changes were needed