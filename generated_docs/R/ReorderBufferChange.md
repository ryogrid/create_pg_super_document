# ReorderBufferChange

## Location
[src/include/replication/reorderbuffer.h:71-159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/reorderbuffer.h#L71-L159)

## Overview
ReorderBufferChange represents a single modification operation (insert, update, delete, truncate, or internal operation) in PostgreSQL's logical replication system, encapsulating all necessary data to describe and replay the change.

## Definition


## Detailed Description
ReorderBufferChange is the fundamental data structure used in PostgreSQL's logical replication system to represent any type of change that occurs within a transaction. It uses a union to efficiently store different types of change data depending on the operation type. The structure supports tuple-level changes (INSERT, UPDATE, DELETE), DDL operations like TRUNCATE, logical replication messages, and internal operations for snapshot and command ID management. Each change is linked to its parent transaction and maintains ordering information through LSN values.

## Parameters / Member Variables
- : Log Sequence Number indicating the WAL position where this change was recorded
- : Type of change (INSERT, UPDATE, DELETE, TRUNCATE, SNAPSHOT, etc.) defined by ReorderBufferChangeType
- : Pointer to the ReorderBufferTXN that contains this change
- : Replication origin identifier for tracking change source
- : File locator for the relation being modified (for tuple operations)
- : Flag indicating whether TOAST chunks should be cleared after processing
- : Previous version of the tuple (valid for UPDATE and DELETE)
- : New version of the tuple (valid for INSERT and UPDATE)
- : Number of relations being truncated
- : Whether truncation should cascade to dependent objects
- : Whether sequences should be restarted after truncation
- : Array of relation OIDs to truncate
- : Prefix string for logical replication messages
- : Size of the message content
- : Message content for logical replication
- : New snapshot for internal snapshot changes
- : Command ID for internal command tracking
- : Relation locator for tuple CID mapping
- : Item pointer for tuple identification
- : Minimum command ID for tuple visibility
- : Maximum command ID for tuple visibility
- : Combined command ID for complex visibility rules
- : Number of invalidation messages
- : Array of cache invalidation messages
- : Doubly-linked list node for organizing changes within transactions

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