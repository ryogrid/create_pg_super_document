# MXOffsetToMemberPage

## Location
src/backend/access/transam/multixact.c: 172 - 177

## Overview
Converts a MultiXact offset to the corresponding page number in the MultiXact members SLRU buffer.

## Definition
```c
static inline int64 MXOffsetToMemberPage(MultiXactOffset offset)
```

## Detailed Description
This inline function calculates which page within the MultiXact members SLRU contains the member data at a given offset position. Unlike the offset functions that work with MultiXact IDs, this function works with MultiXactOffset values, which are positions within the members SLRU where individual transaction members of a MultiXact are stored.

The function performs integer division to map offset positions to page boundaries within the members SLRU. Each page can hold MULTIXACT_MEMBERS_PER_PAGE member entries, so this function determines which page contains a specific member offset.

This is part of the two-level MultiXact storage system: the offsets SLRU stores starting positions (MultiXactOffset values) for each MultiXact's member list, and the members SLRU stores the actual transaction member data at those offset positions.

## Parameters / Member Variables
- `offset`: The MultiXact offset position for which to calculate the page number in the members SLRU

## Dependencies
- Functions called/Symbols referenced:
  - MULTIXACT_MEMBERS_PER_PAGE (constant defining member entries per page)
  - MultiXactOffset (type definition for offset positions)
- Called from (representative examples):
  - MXOffsetToMemberSegment
  - RecordNewMultiXact
  - OFFSET_WARN_SEGMENTS (called twice)
  - GetMultiXactIdMembers
  - StartupMultiXact
  - TrimMultiXact
  - ExtendMultiXactMember

## Notes and Other Information
- This is a static inline function for performance optimization
- Works with the members SLRU, not the offsets SLRU (unlike the MultiXactIdToOffset* functions)
- Essential for locating individual transaction members within the MultiXact storage system
- The offset parameter represents a position in the global members space, not a MultiXact ID
- Used in conjunction with the offset-to-entry function to provide complete addressing within members pages