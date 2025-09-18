# MultiXactIdToOffsetSegment

## Location
src/backend/access/transam/multixact.c: 124 - 141

## Overview
Converts a MultiXact ID to the corresponding segment number in the MultiXact offsets SLRU file system.

## Definition
```c
static inline int64 MultiXactIdToOffsetSegment(MultiXactId multi)
```

## Detailed Description
This inline function calculates which file segment contains the MultiXact offset data for a given MultiXact ID. SLRU (Simple Least Recently Used) structures organize data into segments, where each segment is a physical file containing multiple pages. This function builds upon MultiXactIdToOffsetPage by further dividing the page number by SLRU_PAGES_PER_SEGMENT to determine the segment number.

The function is essential for file-level operations in the MultiXact system, such as truncation and WAL replay, where operations need to be performed at the segment (file) level rather than individual pages.

## Parameters / Member Variables
- `multi`: The MultiXact ID for which to calculate the segment number

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdToOffsetPage (function to get page number)
  - SLRU_PAGES_PER_SEGMENT (constant defining pages per segment)
  - MultiXactId (type definition)
- Called from (representative examples):
  - TruncateMultiXact (called twice for range operations)
  - multixact_redo (called twice during WAL replay)

## Notes and Other Information
- This is a static inline function for performance optimization
- Represents the highest level of addressing in the MultiXact offsets SLRU hierarchy: segment -> page -> entry
- Used primarily for file-level operations like truncation and WAL replay
- The function chain (MultiXactId -> Segment -> Page -> Entry) provides complete addressing for the SLRU structure
- Essential for managing the physical storage of MultiXact offset data across multiple files