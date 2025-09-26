# stream_write_change

## Location
[src/backend/replication/logical/worker.c:4305-4334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4305-L4334)

## Overview
Serializes a logical replication change to a file using a simple format consisting of length, action code, and message contents.

## Definition
```c
static void stream_write_change(char action, StringInfo s)
```

## Detailed Description
This function writes logical replication change data to the currently open streaming file in a standardized serialization format. The format consists of three components written sequentially: the total length (including action type but excluding the length field itself), the action code character that identifies the message type, and the remaining message contents from the StringInfo buffer (starting from the current cursor position, effectively skipping any transaction ID that may have been consumed). This serialization format enables efficient storage and later retrieval of streaming changes during logical replication.

## Parameters / Member Variables
- `action`: Character code identifying the type of replication message being serialized
- `s`: StringInfo buffer containing the message data to be written (writing starts from s->cursor position)

## Dependencies
- Functions called/Symbols referenced:
  - [BufFileWrite](../B/BufFileWrite.md) (called three times for length, action, and data)
- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md)  
  - [apply_handle_stream_stop](../a/apply_handle_stream_stop.md)
  - [stream_open_and_write_change](stream_open_and_write_change.md)

## Notes and Other Information
- This is a static function, visible only within the worker.c compilation unit
- Uses an assertion to ensure stream_fd is not NULL before writing
- The serialization format is: [length:int][action:char][data:variable]
- The length calculation includes the action character but excludes the length field itself
- Only writes data from s->cursor to s->len, allowing selective writing of StringInfo contents
- This design allows the function to skip transaction ID data that may have been consumed from the buffer
- The format is optimized for sequential writing and later sequential reading during message replay
- Part of the core streaming infrastructure for logical replication message persistence