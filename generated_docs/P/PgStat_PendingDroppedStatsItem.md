# PgStat_PendingDroppedStatsItem

## Location
[src/backend/utils/activity/pgstat_xact.c:21-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L21-L26)

## Overview
A structure representing a pending statistics item that needs to be dropped or created transactionally, used to track statistics changes within transaction context for proper rollback handling.

## Definition

```c
typedef struct PgStat_PendingDroppedStatsItem
{
	xl_xact_stats_item item;
	bool		is_create;
	dlist_node	node;
} PgStat_PendingDroppedStatsItem;
```
## Detailed Description
 is a key data structure in PostgreSQL's transactional statistics system that manages statistics entries that are pending creation or deletion within a transaction context. This structure ensures that statistics changes are properly coordinated with transaction commits and rollbacks, maintaining consistency between the statistics system and the actual database state.

The structure serves as a container for statistics operations that need to be deferred until transaction completion. When statistics objects (like tables, indexes, functions, etc.) are created or dropped within a transaction, these operations are tracked using this structure rather than being applied immediately. This approach allows the statistics system to maintain transactional semantics - if the transaction rolls back, the statistics changes are also rolled back.

This is part of PostgreSQL's cumulative statistics system's transactional integration, which ensures that statistics collection and management remain consistent with the database's ACID properties.

## Parameters / Member Variables
- `item`: An  structure containing the core statistics information including the kind of statistics object, database OID, and object OID. This represents the actual statistics entry being tracked.
- `is_create`: A boolean flag indicating whether this entry represents a statistics creation () or deletion () operation. This determines the action to be taken upon transaction commit.
- `node`: A  structure that allows this item to be linked into doubly-linked lists for efficient management of multiple pending statistics operations within the same transaction context.
## Dependencies
- Functions called/Symbols referenced:
  -  (embedded struct for WAL-compatible stats item representation)
  -  (for doubly-linked list management)

- Called from (representative examples):
  -  (processes pending stats at transaction end)
  -  (handles subtransaction completion)
  -  (retrieves pending drops for WAL logging)
  -  (creates new pending statistics items)

## Notes and Other Information
- This structure is defined in  as part of the internal transactional statistics implementation and is not exposed in public headers
- The use of  ensures that the statistics information is compatible with Write-Ahead Logging (WAL) format requirements
- The doubly-linked list design using  allows for efficient insertion, deletion, and traversal of pending statistics operations
- Statistics operations are accumulated during transaction execution and then processed as a batch during transaction commit or rollback
- The transactional nature ensures that statistics remain consistent even in the presence of concurrent transactions and system failures
- This mechanism is crucial for maintaining accurate system statistics while preserving PostgreSQL's transactional guarantees