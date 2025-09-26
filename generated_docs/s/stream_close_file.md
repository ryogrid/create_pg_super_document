# stream_close_file

## Location
[src/backend/replication/logical/worker.c:4287-4304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4287-L4304)

## Overview
Closes the currently open file containing streamed changes and resets the global stream file descriptor to NULL.

## Definition
```c
static void stream_close_file(void)
```

## Detailed Description
This function provides a simple interface to close the currently active streaming file in PostgreSQL logical replication. It ensures the file descriptor is valid before closing and properly resets the global stream_fd variable to NULL after closure. The function uses an assertion to verify that a file is actually open before attempting to close it, helping to catch programming errors during development.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [BufFileClose](../B/BufFileClose.md)
- Called from (representative examples):
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [stream_stop_internal](stream_stop_internal.md)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)
  - [apply_spooled_messages](../a/apply_spooled_messages.md)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md)

## Notes and Other Information
- This is a static function, visible only within the worker.c compilation unit
- Uses an assertion to ensure stream_fd is not NULL before attempting to close
- Resets the global stream_fd variable to NULL after closing, maintaining clean state
- Called during various transaction lifecycle events (prepare, stop, abort, commit)
- Simple wrapper around BufFileClose that also handles state management
- Part of the streaming file management infrastructure for logical replication
- The assertion helps catch bugs where the function might be called when no file is open