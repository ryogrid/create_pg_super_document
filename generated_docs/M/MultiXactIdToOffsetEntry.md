# MultiXactIdToOffsetEntry

## Location
[src/backend/access/transam/multixact.c:118-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L118-L123)

## Overview
Calculates the entry index within a MultiXact offsets SLRU page for a given MultiXact ID.

## Definition
```c
static inline int MultiXactIdToOffsetEntry(MultiXactId multi)
```

## Detailed Description
This inline function computes the specific entry position within an offsets SLRU page that corresponds to a given MultiXact ID. While MultiXactIdToOffsetPage determines which page contains the offset data, this function pinpoints the exact entry within that page using modulo arithmetic.

The function works in tandem with MultiXactIdToOffsetPage to provide complete addressing within the MultiXact offsets SLRU structure. Each page contains MULTIXACT_OFFSETS_PER_PAGE entries, and this function calculates the remainder when dividing the MultiXact ID by this constant.

## Parameters / Member Variables
- `multi`: The MultiXact ID for which to calculate the entry index within its page

## Dependencies
- Functions called/Symbols referenced:
  - MULTIXACT_OFFSETS_PER_PAGE (constant defining entries per page)
  - MultiXactId (type definition)
- Called from (representative examples):
  - [RecordNewMultiXact](../R/RecordNewMultiXact.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)  
  - [TrimMultiXact](../T/TrimMultiXact.md)
  - [ExtendMultiXactOffset](../E/ExtendMultiXactOffset.md)
  - [find_multixact_start](../f/find_multixact_start.md)

## Notes and Other Information
- This is a static inline function for performance optimization
- Returns an integer index from 0 to (MULTIXACT_OFFSETS_PER_PAGE - 1)
- Used together with MultiXactIdToOffsetPage to fully address offset entries in the SLRU buffer
- Essential for precise location of MultiXact offset data within SLRU pages
- The modulo operation ensures the result always falls within valid page entry bounds