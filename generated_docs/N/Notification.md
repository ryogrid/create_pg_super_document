# Notification

## Location
[src/backend/commands/async.c:381-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L381-L387)

## Overview
The Notification structure represents a pending NOTIFY event in PostgreSQL's asynchronous notification system, storing both the channel name and optional payload data for a notification that will be delivered when the current transaction commits.

## Definition

```c
typedef struct Notification
{
	uint16		channel_len;	/* length of channel-name string */
	uint16		payload_len;	/* length of payload string */
	/* null-terminated channel name, then null-terminated payload follow */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} Notification;
```
## Detailed Description
The Notification structure is a core component of PostgreSQL's asynchronous notification system. It represents a single NOTIFY event that has been issued within a transaction but not yet delivered. The structure uses a flexible array member design to store variable-length channel names and payload data in a single memory allocation.

This structure is part of the outbound notify state management system that ensures NOTIFY commands are only executed upon transaction commit, not when initially issued. Each Notification contains both the channel name and an optional payload, with their lengths explicitly tracked to handle binary data properly.

The structure is designed for memory efficiency, storing the channel name and payload data consecutively in the flexible array member , with both strings being null-terminated for compatibility with C string functions.

## Parameters / Member Variables
- : Length of the channel name string (not including null terminator)
- : Length of the payload string (not including null terminator) 
- : Flexible array member containing the null-terminated channel name followed immediately by the null-terminated payload string

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array member declaration)
  
- Called from (representative examples):
  - NotificationHash (hash table operations)
  - [Async_Notify](../A/Async_Notify.md) (main notification processing)
  - [asyncQueueNotificationToEntry](../a/asyncQueueNotificationToEntry.md) (queue management)
  - [asyncQueueAddEntries](../a/asyncQueueAddEntries.md) (queue processing)
  - [AtSubCommit_Notify](../A/AtSubCommit_Notify.md) (subtransaction handling)
  - [AsyncExistsPendingNotify](../A/AsyncExistsPendingNotify.md) (duplicate detection)
  - [AddEventToPendingNotifies](../A/AddEventToPendingNotifies.md) (event management)
  - [notification_hash](../n/notification_hash.md) (hash function)
  - [notification_match](../n/notification_match.md) (comparison function)

## Notes and Other Information
- This structure is kept in CurTransactionContext and is part of a list of pending notifications
- Duplicate notifications within the same transaction are detected and discarded using hash table lookups
- In subtransactions, each level maintains its own notification list that gets merged with the parent upon successful commit
- The structure supports both channel-only notifications (empty payload) and notifications with payload data
- Memory layout: [Notification header][channel_name\0][payload\0]
- The notification system guarantees delivery order matches the order of NOTIFY commands issued
- Failed subtransactions simply discard their notification lists without affecting parent transactions