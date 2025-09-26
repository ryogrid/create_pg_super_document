# xl_xact_invals

## Location
[src/include/access/xact.h:296-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L296-L300)

## Overview
A structure used in PostgreSQL's WAL to store shared invalidation messages that need to be processed during transaction commit, ensuring cache coherency across the system.

## Definition

```c
typedef struct xl_xact_invals
{
	int			nmsgs;			/* number of shared inval msgs */
	SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_invals;
```
## Detailed Description
The xl_xact_invals structure is a critical component of PostgreSQL's cache invalidation system, used in Write-Ahead Logging to record shared invalidation messages that must be processed when a transaction commits. This structure ensures that all backend processes maintain consistent cache states by logging invalidation messages that inform them about changes to cached data such as system catalogs, relation caches, and other shared structures. The flexible array design allows efficient storage of variable numbers of invalidation messages within a single WAL record.

## Parameters / Member Variables
- `nmsgs`: An integer specifying the number of shared invalidation messages contained in the msgs array
- `msgs[FLEXIBLE_ARRAY_MEMBER]`: A flexible array of SharedInvalidationMessage structures containing the actual invalidation messages to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md)
  - FLEXIBLE_ARRAY_MEMBER

- Called from (representative examples):
  - [ParseCommitRecord](../P/ParseCommitRecord.md) (in xactdesc.c:100)
  - [xact_desc](xact_desc.md) (in xactdesc.c:478)
  - [XactLogCommitRecord](../X/XactLogCommitRecord.md) (in xact.c:5767)
  - [xact_decode](xact_decode.md) (in decode.c:284, 287)
  - [LogLogicalInvalidations](../L/LogLogicalInvalidations.md) (in inval.c:1609)
  - MinSizeOfXactInvals (in xact.h:301)

## Notes and Other Information
- Essential for maintaining cache coherency across PostgreSQL backend processes
- Used primarily during transaction commit operations to propagate cache invalidations
- Closely integrated with PostgreSQL's shared invalidation messaging system
- Critical for ensuring data consistency in multi-process database environments
- Part of the logical replication decoding process for handling invalidation messages
- The structure is defined in src/include/access/xact.h at lines 296-300