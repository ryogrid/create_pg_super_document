# pgNotify

## Location
[src/interfaces/libpq/libpq-fe.h:212-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-fe.h#L212-L218)

## Overview
pgNotify represents the occurrence of a NOTIFY message from a PostgreSQL backend, containing the notification condition name, originating process ID, and optional payload data.

## Definition

```c
typedef struct pgNotify
{
	char	   *relname;		/* notification condition name */
	int			be_pid;			/* process ID of notifying server process */
	char	   *extra;			/* notification parameter */
	/* Fields below here are private to libpq; apps should not use 'em */
	struct pgNotify *next;		/* list link */
} PGnotify;
```
## Detailed Description
pgNotify is a simple structure that encapsulates asynchronous notification messages sent by PostgreSQL servers via the NOTIFY command. Unlike most other libpq structures, pgNotify is intentionally not opaque - its public fields are directly accessible to applications because the structure is simple and unlikely to change.

The PostgreSQL NOTIFY/LISTEN mechanism allows database sessions to send and receive asynchronous messages. When a backend executes a NOTIFY command, all connected clients that have issued a LISTEN for that notification condition will receive a pgNotify message.

Key features:
- **Asynchronous Communication**: Enables real-time messaging between database sessions
- **Condition-Based**: Messages are tagged with condition names for selective listening
- **Payload Support**: Can carry additional data beyond just the notification name
- **Process Identification**: Identifies which backend process sent the notification
- **Queue Management**: Notifications are queued until retrieved by the client

The structure supports both simple notifications (just a condition name) and rich notifications with additional payload data.

## Parameters / Member Variables
- **relname**:  - The notification condition name that was used in the NOTIFY command; this is what clients LISTEN for
- **be_pid**:  - Process ID of the PostgreSQL backend that sent the notification (since PostgreSQL 6.4+)
- **extra**:  - Optional notification payload/parameter data sent with the NOTIFY command
- **next**:  - Private field used by libpq to maintain a linked list of pending notifications

## Dependencies
- Functions called/Symbols referenced:
  - [pgNotify](pgNotify.md) (self-referential for linked list)
- Called from (representative examples):
  - [PQnotifies](../P/PQnotifies.md) - Retrieves the next pending notification from the queue
  - NOTIFY SQL commands - Generate notifications that result in pgNotify structures
  - LISTEN SQL commands - Set up to receive notifications

## Notes and Other Information
- Unlike other libpq structures, pgNotify is not opaque - applications can directly access its public fields
- The structure is designed to be simple and stable since it's part of the public API
- Notifications are queued in the PGconn until retrieved via PQnotifies()
- Applications must free pgNotify structures using PQfreemem() when done
- The be_pid field changed behavior in PostgreSQL 6.4 - earlier versions always reported the receiving backend's PID
- The 'next' field is private and should not be accessed by applications
- Supports the PostgreSQL pub/sub messaging pattern through NOTIFY/LISTEN
- Notifications can be sent with or without payload data in the 'extra' field
- Thread safety: Individual pgNotify structures are safe to read from multiple threads once retrieved