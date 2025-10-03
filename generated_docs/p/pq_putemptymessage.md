# pq_putemptymessage

## Location
[src/backend/libpq/pqformat.c:388-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L388-L398)

## Overview
A convenience function for sending messages with no body content to PostgreSQL clients, containing only a message type header.

## Definition

```c
void
pq_putemptymessage(char msgtype)
```
## Detailed Description
The  function provides a simple and convenient way to send messages that consist only of a message type with no additional data payload. It is essentially a wrapper around  that specifically handles the common case of zero-length messages by passing NULL for the data pointer and 0 for the length.

This function is frequently used in PostgreSQL's protocol implementation for sending acknowledgments, completion notifications, and other control messages that don't require any data beyond their message type identifier.

## Parameters / Member Variables
- `msgtype`: The message type character that identifies what kind of message is being sent
## Dependencies
- Functions called/Symbols referenced:
  - pq_putmessage (with NULL data and 0 length)
- Called from (representative examples):
  - [SendCopyDone](../S/SendCopyDone.md) (src/backend/backup/basebackup_copy.c:333)
  - [SendCopyEnd](../S/SendCopyEnd.md) (src/backend/commands/copyto.c:155)
  - [NullCommand](../N/NullCommand.md) (src/backend/tcop/dest.c:227)
  - [exec_parse_message](../e/exec_parse_message.md) (src/backend/tcop/postgres.c:1596)
  - [exec_bind_message](../e/exec_bind_message.md) (src/backend/tcop/postgres.c:2062)
  - [PostgresMain](../P/PostgresMain.md) (src/backend/tcop/postgres.c:4915)

## Notes and Other Information
- Simplifies the common pattern of sending header-only messages in the PostgreSQL protocol
- Eliminates the need to explicitly pass NULL and 0 parameters to pq_putmessage
- Commonly used for protocol state transitions and command completion acknowledgments
- Part of PostgreSQL's client-server communication infrastructure
- Helps maintain clean and readable code when sending simple control messages
- The message consists only of the protocol message type byte with no additional payload

## Simplified Source

```c
// Simplified version of pq_putemptymessage
void pq_putemptymessage(char msgtype) {
    // Send a message with only the type header and no data payload
    pq_putmessage(msgtype, NULL, 0);
}
```

Key simplifications made:
- This function was already very simple, containing only a single function call
- Added explanatory comment to clarify the purpose
- The core logic is a direct wrapper around pq_putmessage with empty data
- No error handling or complex logic to simplify