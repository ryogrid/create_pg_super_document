# bbsink_forward_manifest_contents

## Location
[src/backend/backup/basebackup_sink.c:89-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_sink.c#L89-L100)

## Overview
A forwarding function that passes manifest content processing to the next backup sink in a chain, used in PostgreSQL's base backup system to forward manifest data through a chain of backup sinks.

## Definition

```c
void
bbsink_forward_manifest_contents(bbsink *sink, size_t len)
```
## Detailed Description
This function is part of PostgreSQL's base backup sink forwarding mechanism. It forwards the manifest_contents callback to the next sink in a chained configuration of backup sinks. The function ensures that manifest content data is properly propagated through the sink chain by calling the appropriate manifest_contents operation on the next sink.

The function performs several assertions to validate the sink chain state:
- Ensures there is a next sink in the chain
- Verifies that buffers are properly shared between sinks in the chain
- Validates that buffer lengths match between chained sinks

This forwarding pattern allows for composition of different backup sink behaviors, such as combining throttling, server communication, and other processing steps.

## Parameters / Member Variables
- `*sink`: Pointer to the current bbsink structure in the chain
- `len`: Size of the manifest content data to process (must be > 0 and <= buffer length)
## Dependencies
- Functions called/Symbols referenced:
  - [bbsink_manifest_contents](bbsink_manifest_contents.md)
  - [bbsink](bbsink.md) (structure type)
- Called from (representative examples):
  - [bbsink_server_manifest_contents](bbsink_server_manifest_contents.md)
  - [bbsink_throttle_manifest_contents](bbsink_throttle_manifest_contents.md)

## Notes and Other Information
- The function expects that buffers are shared between chained sinks, allowing efficient data forwarding without copying
- This is part of a callback-based architecture where different sink implementations can be chained together
- The function includes assertions to ensure proper sink chain configuration and shared buffer state
- Used specifically for processing backup manifest contents during PostgreSQL base backup operations

## Simplified Source

```c
// Simplified version of bbsink_forward_manifest_contents
void bbsink_forward_manifest_contents(bbsink *sink, size_t len) {
    // Validate chain and buffer sharing
    Assert(sink->bbs_next != NULL);
    Assert(sink->bbs_buffer == sink->bbs_next->bbs_buffer);
    Assert(sink->bbs_buffer_length == sink->bbs_next->bbs_buffer_length);

    // Forward manifest contents to next sink
    bbsink_manifest_contents(sink->bbs_next, len);
}
```

Key simplifications made:
- Preserved essential buffer sharing validation
- Maintained manifest contents forwarding with length
- Focused on core manifest data forwarding with shared buffers