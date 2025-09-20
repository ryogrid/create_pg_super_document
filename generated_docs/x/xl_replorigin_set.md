# xl_replorigin_set

## Location
[src/include/replication/origin.h:18-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/origin.h#L18-L23)

## Overview
WAL record structure that stores information for setting replication origin progress during recovery or replication operations.

## Definition

```c
typedef struct xl_replorigin_set
{
	XLogRecPtr	remote_lsn;
	RepOriginId node_id;
	bool		force;
} xl_replorigin_set;
```
## Detailed Description
The  structure is used in PostgreSQL's Write-Ahead Logging (WAL) system to record replication origin progress updates. This structure is written to WAL when  is called with WAL logging enabled. During recovery or when applying WAL records, this information is used to restore the replication progress state of various replication origins.

The structure represents a WAL record of type  and contains the essential information needed to track the progress of logical replication from a specific origin node.

## Parameters / Member Variables
- : The LSN (Log Sequence Number) on the remote/origin server that corresponds to this progress point
- : The unique identifier of the replication origin node (of type RepOriginId, which is uint16)
- : Boolean flag indicating whether to allow backward movement of the replication progress (normally progress only moves forward)

## Dependencies
- Functions called/Symbols referenced:
  - RepOriginId (type definition)
  - XLogRecPtr (type for LSN values)

- Called from (representative examples):
  - [replorigin_desc](../r/replorigin_desc.md) (WAL record description function)
  - [replorigin_redo](../r/replorigin_redo.md) (WAL record replay function) 
  - [replorigin_advance](../r/replorigin_advance.md) (when WAL logging is enabled)

## Notes and Other Information
- This structure is part of PostgreSQL's logical replication infrastructure
- The  parameter allows for special cases where replication progress needs to move backward, which is normally not allowed
- During WAL replay, this record is processed by  which calls  to update the in-memory replication state
- The structure is defined in  alongside other replication origin related definitions
- WAL record type constant:  (0x00)