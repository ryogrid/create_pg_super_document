# bbsink_forward_begin_backup

## Location
[src/backend/backup/basebackup_sink.c:24-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_sink.c#L24-L36)

## Overview
Forwards the begin_backup callback to the next bbsink in a chain, enabling buffer sharing between the current and successor bbsink implementations.

## Definition
```c
void bbsink_forward_begin_backup(bbsink *sink)
```

## Detailed Description
This function implements a forwarding pattern for the begin_backup callback in PostgreSQL's base backup sink infrastructure. It is designed for bbsink implementations that want to share a buffer with their successor bbsink rather than managing their own separate buffer. The function forwards the begin_backup call to the next bbsink in the chain (sink->bbs_next) and then adopts the successor's buffer as its own.

The function ensures that the bbsink chain is properly initialized and that buffer sharing is established correctly. This is particularly useful for bbsinks that perform transformations or filtering on the backup data stream without needing to maintain separate buffer space.

## Parameters / Member Variables
- `sink`: Pointer to the bbsink structure that is forwarding the begin_backup operation to its successor

## Dependencies
- Functions called/Symbols referenced:
  - [bbsink_begin_backup](bbsink_begin_backup.md)
  - [bbsink](bbsink.md) (type reference)
- Called from (representative examples):
  - [bbsink_progress_begin_backup](bbsink_progress_begin_backup.md) (src/backend/backup/basebackup_progress.c:107)
  - [bbsink_throttle_begin_backup](bbsink_throttle_begin_backup.md) (src/backend/backup/basebackup_throttle.c:100)

## Notes and Other Information
- This implementation should only be used when the bbsink wants to share a buffer with its successor
- The function performs assertions to ensure that both bbs_next and bbs_state are properly initialized
- After forwarding the call, the current bbsink adopts the buffer from its successor (sink->bbs_buffer = sink->bbs_next->bbs_buffer)
- This is a utility function that reduces code duplication across different bbsink implementations that need similar forwarding behavior

## Simplified Source

```c
// Simplified version of bbsink_forward_begin_backup
void bbsink_forward_begin_backup(bbsink *sink) {
    // Validate chain and state
    Assert(sink->bbs_next != NULL);
    Assert(sink->bbs_state != NULL);

    // Forward begin backup to next sink
    bbsink_begin_backup(sink->bbs_next, sink->bbs_state,
                       sink->bbs_buffer_length);

    // Share buffer with successor
    sink->bbs_buffer = sink->bbs_next->bbs_buffer;
}
```

Key simplifications made:
- Preserved essential validation and buffer sharing
- Maintained forward delegation with state passing
- Focused on core backup initialization forwarding