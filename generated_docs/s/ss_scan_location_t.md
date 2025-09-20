# ss_scan_location_t

## Location
[src/backend/access/common/syncscan.c:91-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/syncscan.c#L91-L95)

## Overview
A structure that stores the synchronous scan location information for a specific relation, used in PostgreSQL's synchronized scan optimization to coordinate multiple sequential scans.

## Definition

```c
typedef struct ss_scan_location_t
{
	RelFileLocator relfilelocator;	/* identity of a relation */
	BlockNumber location;		/* last-reported location in the relation */
} ss_scan_location_t;
```
## Detailed Description
The  structure is a core component of PostgreSQL's synchronized scan mechanism. It stores the location information for a relation being scanned, allowing multiple concurrent sequential scans to coordinate and potentially start from the same position to improve buffer cache efficiency. This structure is designed to work within a fixed-size shared memory allocation as part of a doubly-linked LRU (Least Recently Used) cache system.

The synchronized scan optimization helps reduce I/O by allowing multiple sequential scans of the same table to coordinate their starting positions. When a new scan begins, it can check if another scan is already in progress on the same relation and start from that location instead of from the beginning.

## Parameters / Member Variables
- : A RelFileLocator structure that uniquely identifies a relation (table) in the database system
- : A BlockNumber representing the last-reported block position in the relation where scanning activity occurred

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (type)
  - BlockNumber (type)
- Called from (representative examples):
  - [ss_lru_item_t](ss_lru_item_t.md) (used as a member)

## Notes and Other Information
- This structure is part of PostgreSQL's synchronized scan infrastructure located in src/backend/access/common/syncscan.c
- It's designed to be used within a fixed-size shared memory area as part of an LRU cache
- The structure helps coordinate multiple sequential table scans to improve buffer cache utilization
- The RelFileLocator provides a unique identifier for the relation being scanned across different databases and tablespaces