# bbsink_throttle_archive_contents

## Location
[src/backend/backup/basebackup_throttle.c:110-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_throttle.c#L110-L120)

## Overview
Applies throttling to archive contents data transfer and forwards the data to the next sink in the basebackup pipeline.

## Definition

```c
static void
bbsink_throttle_archive_contents(bbsink *sink, size_t len)
```
## Detailed Description
The  function is responsible for handling archive contents data transfer while applying bandwidth throttling. It operates in two sequential steps: first applying the throttling mechanism to control the transfer rate based on the specified data length, then forwarding the archive contents to the next sink in the pipeline.

This function serves as the main data flow control point for archive contents, ensuring that the transfer rate doesn't exceed the configured limits while maintaining the pipeline architecture by passing data to subsequent sinks.

## Parameters / Member Variables
- `*sink`: Pointer to the base bbsink structure, cast to bbsink_throttle for accessing throttling-specific functionality
- `len`: Size of the archive contents data being transferred (in bytes)
## Dependencies
- Functions called/Symbols referenced:
  -  (applies throttling mechanism based on data length)
  -  (forwards data to next sink in pipeline)
  -  (structure type for casting)
- Called from (representative examples):
  - Used as callback through bbsink_throttle_ops function pointer table

## Notes and Other Information
- This is a static function, used only within the basebackup_throttle.c module
- Part of the bbsink operation callback interface for handling archive contents
- The throttling is applied before forwarding, ensuring rate limits are enforced
- Works in conjunction with the throttle() function to implement the actual rate limiting logic
- Handles specifically archive contents data, as opposed to manifest or other types of backup data

## Simplified Source

```c
static void bbsink_throttle_archive_contents(bbsink *sink, size_t len) {
    // Apply throttling based on data length
    throttle((bbsink_throttle *) sink, len);

    // Forward archive contents to next sink in pipeline
    bbsink_forward_archive_contents(sink, len);
}
```