# stream_open_and_write_change

## Location
[src/backend/replication/logical/worker.c:4335-4350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4335-L4350)

## Overview
A static function in PostgreSQL's logical replication worker that serializes a replication message to a file for a given transaction, handling file opening and closing automatically.

## Definition

```c
static void
stream_open_and_write_change(TransactionId xid, char action, StringInfo s)
```
## Detailed Description
stream_open_and_write_change is a convenience function that combines file management operations with message writing for streamed transactions in logical replication. It serves as a wrapper around stream_write_change, automatically handling the opening of the target file (if not already open) before writing and closing it afterward. This function is specifically designed for non-streamed transactions (as indicated by the Assert(!in_streamed_transaction) check) and provides a complete write cycle in a single call.

The function follows a simple three-step process:
1. Opens the stream file if not already open using stream_start_internal
2. Writes the change using stream_write_change
3. Closes the stream using stream_stop_internal

This encapsulation ensures proper file lifecycle management and is used primarily for handling stream-related operations like prepare, abort, and commit messages.

## Parameters / Member Variables
- `xid`: TransactionId of the transaction for which the message is being serialized
- `action`: Character representing the type of action/message being written
- `s`: StringInfo containing the serialized message data to be written to the file
## Dependencies
- Functions called/Symbols referenced:
  - [stream_start_internal](stream_start_internal.md) (opens the stream file)
  - [stream_write_change](stream_write_change.md) (writes the actual message data)
  - [stream_stop_internal](stream_stop_internal.md) (closes the stream file)
- Called from (representative examples):
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md) (at src/backend/replication/logical/worker.c:1344)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md) (at src/backend/replication/logical/worker.c:1924)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md) (at src/backend/replication/logical/worker.c:2192)

## Notes and Other Information
- This function includes an assertion to ensure it's not called during an active streamed transaction
- It provides automatic file lifecycle management, making it suitable for one-off message writes
- Used specifically for handling stream control messages (prepare, abort, commit) in logical replication
- Part of PostgreSQL's logical replication worker subsystem for handling large transactions that need to be spilled to disk