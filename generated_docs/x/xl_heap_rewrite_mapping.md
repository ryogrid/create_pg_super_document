# xl_heap_rewrite_mapping

## Location
[src/include/access/heapam_xlog.h:467-475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L467-L475)

## Overview
A WAL record structure that logs tuple mapping information during heap rewrites for logical replication consistency.

## Definition

```c
typedef struct xl_heap_rewrite_mapping
{
	TransactionId mapped_xid;	/* xid that might need to see the row */
	Oid			mapped_db;		/* DbOid or InvalidOid for shared rels */
	Oid			mapped_rel;		/* Oid of the mapped relation */
	off_t		offset;			/* How far have we written so far */
	uint32		num_mappings;	/* Number of in-memory mappings */
	XLogRecPtr	start_lsn;		/* Insert LSN at begin of rewrite */
} xl_heap_rewrite_mapping;
```
## Detailed Description
The xl_heap_rewrite_mapping structure is used in PostgreSQL's logical replication system to maintain tuple mapping information during heap rewrites (such as during ALTER TABLE operations that require table reconstruction). When a table is rewritten, the physical locations of tuples change, but logical replication needs to maintain the mapping between old and new tuple locations to ensure consistency across replicated systems.

This structure is logged as part of the WAL stream specifically for logical replication consumers, allowing them to track how tuples have been relocated during the rewrite process. It's essential for maintaining the logical replication stream's integrity when physical table restructuring occurs.

## Parameters / Member Variables
- : Transaction ID that might need to see the row - identifies which transaction's changes this mapping applies to
- : Database OID where the relation resides, or InvalidOid for shared relations (system catalogs)
- : Object ID of the relation being rewritten
- : Current offset indicating how much mapping data has been written so far
- : Number of tuple mappings currently stored in memory for this rewrite operation
- : LSN (Log Sequence Number) at the beginning of the rewrite operation for tracking purposes

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (type)
  - Oid (type) 
  - off_t (type)
  - uint32 (type)
  - XLogRecPtr (type)
- Called from (representative examples):
  - [logical_heap_rewrite_flush_mappings](../l/logical_heap_rewrite_flush_mappings.md) (src/backend/access/heap/rewriteheap.c:827)
  - heap_xlog_logical_rewrite (src/backend/access/heap/rewriteheap.c:1077,1081)

## Notes and Other Information
- Specifically designed for logical replication - not used in physical replication or crash recovery
- Critical for maintaining logical replication consistency during DDL operations that rewrite tables
- The offset and num_mappings fields help track the progress and state of the rewrite operation
- Used in conjunction with heap rewrite operations during ALTER TABLE and similar DDL commands
- Part of PostgreSQL's logical decoding infrastructure to ensure replicated databases remain consistent during schema changes