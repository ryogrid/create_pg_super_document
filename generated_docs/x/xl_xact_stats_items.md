# xl_xact_stats_items

## Location
[src/include/access/xact.h:289-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L289-L293)

## Overview
A structure used in PostgreSQL's WAL to store multiple transactionally dropped statistics entries as a collection, enabling efficient logging of statistics cleanup operations during transaction processing.

## Definition

```c
typedef struct xl_xact_stats_items
{
	int			nitems;
	xl_xact_stats_item items[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_stats_items;
```
## Detailed Description
The xl_xact_stats_items structure serves as a container for multiple xl_xact_stats_item entries in PostgreSQL's Write-Ahead Logging system. This structure is designed to efficiently handle transactions that affect multiple statistics objects by storing them in a single WAL record. The use of a flexible array member allows the structure to accommodate varying numbers of statistics items without requiring separate memory allocations, making it both memory-efficient and performance-optimized for bulk statistics operations during transaction commit and abort scenarios.

## Parameters / Member Variables
- : An integer specifying the number of statistics items contained in the items array
- : A flexible array of xl_xact_stats_item structures containing the individual statistics entries being processed

## Dependencies
- Functions called/Symbols referenced:
  - xl_xact_stats_item
  - FLEXIBLE_ARRAY_MEMBER

- Called from (representative examples):
  - ParseCommitRecord (in xactdesc.c:89)
  - ParseAbortRecord (in xactdesc.c:195)
  - XactLogCommitRecord (in xact.c:5766)
  - XactLogAbortRecord (in xact.c:5935)
  - MinSizeOfXactStatsItems (in xact.h:294)

## Notes and Other Information
- Provides efficient bulk handling of multiple statistics entries in a single WAL record
- Uses flexible array member design for optimal memory usage and performance
- Critical component in PostgreSQL's transactional statistics management system
- Ensures atomicity when multiple statistics objects are affected by a single transaction
- Closely integrated with transaction commit and abort logging mechanisms
- The structure is defined in src/include/access/xact.h at lines 289-293