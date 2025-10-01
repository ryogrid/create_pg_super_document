# PerformMembersTruncation

## Location
[src/backend/access/transam/multixact.c:3040-3068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3040-L3068)

## Overview
PerformMembersTruncation deletes MultiXact member segments in a specified range, handling the cleanup of member data during MultiXact truncation operations.

## Definition
static void PerformMembersTruncation(MultiXactOffset oldestOffset, MultiXactOffset newOldestOffset)

## Detailed Description
This function performs the actual deletion of MultiXact member segments within a specified offset range. Unlike the offsets SLRU which can use SimpleLruTruncate(), the members SLRU requires special handling because it can be filled to almost the full range at once. The function computes the segment range to delete using offset values and systematically removes segments while handling wraparound correctly.

The function deletes all segments in the range [oldestOffset, newOldestOffset) but preserves the last segment as it may still contain partially valid data. This careful approach ensures that no valid member data is accidentally deleted during truncation operations.

## Parameters / Member Variables
- `oldestOffset`: Starting MultiXact offset for the truncation range (oldest data to be removed)
- `newOldestOffset`: Ending MultiXact offset for the truncation range (new oldest data boundary)

## Dependencies
- Functions called/Symbols referenced:
  - [MXOffsetToMemberSegment](../M/MXOffsetToMemberSegment.md)
  - [SlruDeleteSegment](../S/SlruDeleteSegment.md)
  - MultiXactMemberCtl
  - MaxMultiXactOffset
  - DEBUG2
- Called from (representative examples):
  - [TruncateMultiXact](../T/TruncateMultiXact.md)
  - [multixact_redo](../m/multixact_redo.md)

## Notes and Other Information
- Handles segment wraparound by checking against maxsegment and resetting to 0
- Uses DEBUG2 logging to trace segment deletion operations
- More complex than offset truncation due to the different filling patterns of member data
- Part of the MultiXact cleanup system that manages transaction membership information
- Preserves the last segment in the range to avoid deleting partially valid data

## Simplified Source
```c
static void PerformMembersTruncation(MultiXactOffset oldestOffset, MultiXactOffset newOldestOffset) {
    // Calculate segment range to delete
    const int64 maxsegment = MXOffsetToMemberSegment(MaxMultiXactOffset);
    int64 startsegment = MXOffsetToMemberSegment(oldestOffset);
    int64 endsegment = MXOffsetToMemberSegment(newOldestOffset);
    int64 segment = startsegment;

    // Delete all segments except the last one (may contain valid data)
    while (segment != endsegment) {
        elog(DEBUG2, "truncating multixact members segment %llx", (unsigned long long) segment);
        SlruDeleteSegment(MultiXactMemberCtl, segment);

        // Handle wraparound: reset to 0 after max segment
        if (segment == maxsegment)
            segment = 0;
        else
            segment += 1;
    }
}
```