# MultiXactMemberPagePrecedes

## Location
[src/backend/access/transam/multixact.c:3289-3308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3289-L3308)

## Overview
MultiXactMemberPagePrecedes determines whether one MultiXact member page number is "older" than another for truncation purposes, using a page-based comparison method.

## Definition

```c
static bool
MultiXactMemberPagePrecedes(int64 page1, int64 page2)
```
## Detailed Description
This function compares two MultiXact member page numbers to determine their relative age for cleanup and truncation operations. It converts page numbers to their corresponding MultiXact offset ranges and uses MultiXactOffsetPrecedes to determine precedence. The function ensures that page1 precedes page2 by checking that the starting offset of page1 precedes both the starting offset of page2 and the ending offset of page2's range.

The function is specifically designed for MultiXact member pages, which store the actual member information (transaction IDs and lock modes) referenced by MultiXact IDs. Unlike some other precedence functions, there is no "invalid page number" concept, so the comparison uses the page numbers directly.

## Parameters / Member Variables
- : First page number to compare (int64)
- : Second page number to compare (int64)

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactOffsetPrecedes](MultiXactOffsetPrecedes.md)
  - MultiXactOffset (type)
  - MULTIXACT_MEMBERS_PER_PAGE (constant)
- Called from (representative examples):
  - debug_elog6
  - [MultiXactShmemInit](MultiXactShmemInit.md)

## Notes and Other Information
- This is a static function internal to multixact.c
- The function performs range-based comparison by converting page numbers to offset ranges
- It ensures that the entire range of page1 precedes the entire range of page2
- Used in MultiXact cleanup and truncation logic to determine which pages can be safely removed
- The comparison logic accounts for the wrap-around nature of MultiXact offsets

## Simplified Source

```c
// Simplified version of MultiXactMemberPagePrecedes
static bool MultiXactMemberPagePrecedes(int64 page1, int64 page2) {
    // Convert page numbers to their starting offset ranges
    MultiXactOffset offset1 = ((MultiXactOffset) page1) * MULTIXACT_MEMBERS_PER_PAGE;
    MultiXactOffset offset2 = ((MultiXactOffset) page2) * MULTIXACT_MEMBERS_PER_PAGE;

    // Check if page1's range entirely precedes page2's range
    return (MultiXactOffsetPrecedes(offset1, offset2) &&
            MultiXactOffsetPrecedes(offset1, offset2 + MULTIXACT_MEMBERS_PER_PAGE - 1));
}
```

Key simplifications made:
- Added comments explaining the offset conversion logic
- Clarified the range-based comparison approach
- Preserved the essential dual precedence check
- Maintained the original logic for wrap-around handling