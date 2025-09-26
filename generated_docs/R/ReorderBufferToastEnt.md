# ReorderBufferToastEnt

## Location
[src/backend/replication/logical/reorderbuffer.c:176-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L176-L186)

## Overview
ReorderBufferToastEnt is a structure that manages TOAST (The Oversized-Attribute Storage Technique) data reconstruction during logical replication in PostgreSQL.

## Definition

```c
typedef struct ReorderBufferToastEnt
{
	Oid			chunk_id;		/* toast_table.chunk_id */
	int32		last_chunk_seq; /* toast_table.chunk_seq of the last chunk we
								 * have seen */
	Size		num_chunks;		/* number of chunks we've already seen */
	Size		size;			/* combined size of chunks seen */
	dlist_head	chunks;			/* linked list of chunks */
	struct varlena *reconstructed;	/* reconstructed varlena now pointed to in
									 * main tup */
} ReorderBufferToastEnt;
```
## Detailed Description
This structure is responsible for managing the reconstruction of TOAST data during logical replication. When large attributes are stored using PostgreSQL's TOAST mechanism, they are split into multiple chunks. During logical decoding, these chunks need to be reassembled in the correct order to reconstruct the original value. The ReorderBufferToastEnt tracks the state of this reconstruction process, collecting chunks as they are encountered and maintaining metadata about the reconstruction progress.

## Parameters / Member Variables
- : The OID identifier from the toast_table.chunk_id, uniquely identifying this TOAST entry
- : The sequence number of the most recently processed chunk from toast_table.chunk_seq
- : Count of the total number of chunks that have been collected so far
- : The cumulative size in bytes of all chunks seen for this TOAST entry
- : Doubly-linked list head managing the collection of individual TOAST chunks
- : Pointer to the fully reconstructed varlena structure once all chunks are assembled

## Dependencies
- Functions called/Symbols referenced:
  - dlist_head
  - varlena
- Called from (representative examples):
  - ReorderBufferToastInitHash
  - ReorderBufferToastAppendChunk
  - ReorderBufferToastReplace
  - ReorderBufferToastReset

## Notes and Other Information
- Essential for handling large attribute values that exceed the inline storage threshold in logical replication
- The reconstruction process must handle chunks arriving out of order during replication
- Memory management is critical as reconstructed TOAST values can be very large
- Used internally by the logical replication system to ensure complete and correct reconstruction of oversized attributes
- The structure maintains state until all chunks for a particular TOAST value are collected and can be reassembled