# ReorderBufferChange

## Location
[src/include/replication/reorderbuffer.h:71-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/reorderbuffer.h#L71-L159)

## Overview
ReorderBufferChange represents a single modification operation (insert, update, delete, truncate, or internal operation) in PostgreSQL's logical replication system, encapsulating all necessary data to describe and replay the change.

## Definition

```c
typedef struct ReorderBufferChange
{
	XLogRecPtr	lsn;

	/* The type of change. */
	ReorderBufferChangeType action;

	/* Transaction this change belongs to. */
	struct ReorderBufferTXN *txn;

	RepOriginId origin_id;

	/*
	 * Context data for the change. Which part of the union is valid depends
	 * on action.
	 */
	union
	{
		/* Old, new tuples when action == *_INSERT|UPDATE|DELETE */
		struct
		{
			/* relation that has been changed */
			RelFileLocator rlocator;

			/* no previously reassembled toast chunks are necessary anymore */
			bool		clear_toast_afterwards;

			/* valid for DELETE || UPDATE */
			HeapTuple	oldtuple;
			/* valid for INSERT || UPDATE */
			HeapTuple	newtuple;
		}			tp;

		/*
		 * Truncate data for REORDER_BUFFER_CHANGE_TRUNCATE representing one
		 * set of relations to be truncated.
		 */
		struct
		{
			Size		nrelids;
			bool		cascade;
			bool		restart_seqs;
			Oid		   *relids;
		}			truncate;

		/* Message with arbitrary data. */
		struct
		{
			char	   *prefix;
			Size		message_size;
			char	   *message;
		}			msg;

		/* New snapshot, set when action == *_INTERNAL_SNAPSHOT */
		Snapshot	snapshot;

		/*
		 * New command id for existing snapshot in a catalog changing tx. Set
		 * when action == *_INTERNAL_COMMAND_ID.
		 */
		CommandId	command_id;

		/*
		 * New cid mapping for catalog changing transaction, set when action
		 * == *_INTERNAL_TUPLECID.
		 */
		struct
		{
			RelFileLocator locator;
			ItemPointerData tid;
			CommandId	cmin;
			CommandId	cmax;
			CommandId	combocid;
		}			tuplecid;

		/* Invalidation. */
		struct
		{
			uint32		ninvalidations; /* Number of messages */
			SharedInvalidationMessage *invalidations;	/* invalidation message */
		}			inval;
	}			data;

	/*
	 * While in use this is how a change is linked into a transactions,
	 * otherwise it's the preallocated list.
	 */
	dlist_node	node;
} ReorderBufferChange;
```
## Detailed Description
ReorderBufferChange is the fundamental data structure used in PostgreSQL's logical replication system to represent any type of change that occurs within a transaction. It uses a union to efficiently store different types of change data depending on the operation type. The structure supports tuple-level changes (INSERT, UPDATE, DELETE), DDL operations like TRUNCATE, logical replication messages, and internal operations for snapshot and command ID management. Each change is linked to its parent transaction and maintains ordering information through LSN values.

## Parameters / Member Variables
- `lsn`: Log Sequence Number indicating the WAL position where this change was recorded
- `action`: Type of change (INSERT, UPDATE, DELETE, TRUNCATE, SNAPSHOT, etc.) defined by ReorderBufferChangeType
- `txn`: Pointer to the ReorderBufferTXN that contains this change
- `origin_id`: Replication origin identifier for tracking change source
- `data.tp.rlocator`: File locator for the relation being modified (for tuple operations)
- `data.tp.clear_toast_afterwards`: Flag indicating whether TOAST chunks should be cleared after processing
- `data.tp.oldtuple`: Previous version of the tuple (valid for UPDATE and DELETE)
- `data.tp.newtuple`: New version of the tuple (valid for INSERT and UPDATE)
- `data.truncate.nrelids`: Number of relations being truncated
- `data.truncate.cascade`: Whether truncation should cascade to dependent objects
- `data.truncate.restart_seqs`: Whether sequences should be restarted after truncation
- `data.truncate.relids`: Array of relation OIDs to truncate
- `data.msg.prefix`: Prefix string for logical replication messages
- `data.msg.message_size`: Size of the message content
- `data.msg.message`: Message content for logical replication
- `data.snapshot`: New snapshot for internal snapshot changes
- `data.command_id`: Command ID for internal command tracking
- `data.tuplecid.locator`: Relation locator for tuple CID mapping
- `data.tuplecid.tid`: Item pointer for tuple identification
- `data.tuplecid.cmin`: Minimum command ID for tuple visibility
- `data.tuplecid.cmax`: Maximum command ID for tuple visibility
- `data.tuplecid.combocid`: Combined command ID for complex visibility rules
- `data.inval.ninvalidations`: Number of invalidation messages
- `data.inval.invalidations`: Array of cache invalidation messages
- `node`: Doubly-linked list node for organizing changes within transactions

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferChangeType](ReorderBufferChangeType.md)
  - [ReorderBufferTXN](ReorderBufferTXN.md)
  - RepOriginId
  - CommandId
  - SharedInvalidationMessage
  - [dlist_node](../d/dlist_node.md)
- Called from (representative examples):
  - [ReorderBufferGetChange](ReorderBufferGetChange.md)
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md)
  - [ReorderBufferApplyChange](ReorderBufferApplyChange.md)
  - [DecodeInsert](../D/DecodeInsert.md)/DecodeUpdate/DecodeDelete

## Notes and Other Information
This structure is central to PostgreSQL's logical replication architecture and is used extensively throughout the WAL decoding and logical replication processes. The union design allows efficient memory usage while supporting diverse change types. Changes are typically allocated from a memory pool and linked into transaction change lists for ordered processing during logical replication output.