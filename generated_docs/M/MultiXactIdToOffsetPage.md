# MultiXactIdToOffsetPage

## Location
src/backend/access/transam/multixact.c: 112 - 117

## Overview
Converts a MultiXact ID to the corresponding page number in the MultiXact offsets SLRU (Simple Least Recently Used) buffer.

## Definition
```c
static inline int64 MultiXactIdToOffsetPage(MultiXactId multi)
```

## Detailed Description
This inline function calculates which page within the MultiXact offsets SLRU contains the offset information for a given MultiXact ID. The MultiXact system uses SLRU buffers to manage offset and member data efficiently. Each page can hold a fixed number of MultiXact offset entries (MULTIXACT_OFFSETS_PER_PAGE), so this function performs a simple division to determine the appropriate page number.

The function is part of PostgreSQL's MultiXact system, which tracks multiple transactions that have locks on the same tuple. The offsets SLRU stores the starting positions of each MultiXact's member list in the members SLRU.

## Parameters / Member Variables
- `multi`: The MultiXact ID for which to calculate the page number

## Dependencies
- Functions called/Symbols referenced:
  - MULTIXACT_OFFSETS_PER_PAGE (constant defining entries per page)
  - MultiXactId (type definition)
- Called from (representative examples):
  - [MultiXactIdToOffsetSegment](MultiXactIdToOffsetSegment.md)
  - [RecordNewMultiXact](../R/RecordNewMultiXact.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [MaybeExtendOffsetSlru](MaybeExtendOffsetSlru.md)
  - [StartupMultiXact](../S/StartupMultiXact.md)
  - [TrimMultiXact](../T/TrimMultiXact.md)
  - [ExtendMultiXactOffset](../E/ExtendMultiXactOffset.md)
  - [find_multixact_start](../f/find_multixact_start.md)
  - [PerformOffsetsTruncation](../P/PerformOffsetsTruncation.md)
  - [multixact_redo](../m/multixact_redo.md)

## Notes and Other Information
- This is a static inline function for performance, as it's called frequently during MultiXact operations
- The calculation is straightforward integer division, mapping MultiXact IDs to page boundaries
- Essential for SLRU buffer management in the MultiXact subsystem
- Works in conjunction with MultiXactIdToOffsetEntry to fully locate offset data within pages