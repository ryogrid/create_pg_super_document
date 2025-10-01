# MXOffsetToMemberSegment

## Location
[src/backend/access/transam/multixact.c:178-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L178-L184)

## Overview
Converts a MultiXact offset to the corresponding segment number in the MultiXact members SLRU file system.

## Definition
```c
static inline int64 MXOffsetToMemberSegment(MultiXactOffset offset)
```

## Detailed Description
This inline function calculates which file segment contains the MultiXact member data at a given offset position. Building upon MXOffsetToMemberPage, it further divides the page number by SLRU_PAGES_PER_SEGMENT to determine the segment (file) number within the members SLRU structure.

The function is crucial for file-level operations in the MultiXact members system, such as truncation and WAL replay, where operations need to be performed at the segment level. Each segment represents a physical file in the file system containing multiple pages of member data.

This function works with MultiXactOffset values (positions within the members space) rather than MultiXact IDs, making it part of the members SLRU addressing scheme rather than the offsets SLRU scheme.

## Parameters / Member Variables
- `offset`: The MultiXact offset position for which to calculate the segment number in the members SLRU

## Dependencies
- Functions called/Symbols referenced:
  - [MXOffsetToMemberPage](MXOffsetToMemberPage.md) (function to get page number from offset)
  - SLRU_PAGES_PER_SEGMENT (constant defining pages per segment)
  - MultiXactOffset (type definition for offset positions)
- Called from (representative examples):
  - [PerformMembersTruncation](../P/PerformMembersTruncation.md) (called three times for range operations)
  - [TruncateMultiXact](../T/TruncateMultiXact.md) (called twice for truncation operations)
  - [multixact_redo](../m/multixact_redo.md) (called twice during WAL replay)

## Notes and Other Information
- This is a static inline function for performance optimization
- Represents the highest level of addressing in the MultiXact members SLRU hierarchy: segment -> page -> entry
- Used primarily for file-level operations like truncation and WAL replay on member data
- Works with the members SLRU file system, complementing the offsets SLRU addressing functions
- Essential for managing the physical storage of MultiXact member data across multiple segment files
- The function chain (MultiXactOffset -> Segment -> Page -> Entry) provides complete addressing for the members SLRU structure

## Simplified Source
```c
static inline int64 MXOffsetToMemberSegment(MultiXactOffset offset) {
    // Convert offset to segment number by dividing page number by pages per segment
    return MXOffsetToMemberPage(offset) / SLRU_PAGES_PER_SEGMENT;
}
```