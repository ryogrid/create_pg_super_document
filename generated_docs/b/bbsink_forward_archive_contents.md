# bbsink_forward_archive_contents

## Location
src/backend/backup/basebackup_sink.c: 54 - 65

## Overview
Forwards the archive_contents callback to the next bbsink in a chain, specifically designed for implementations that share buffers with their successor bbsink.

## Definition
```c
void bbsink_forward_archive_contents(bbsink *sink, size_t len)
```

## Detailed Description
This function implements a forwarding pattern for the archive_contents callback within PostgreSQL's base backup sink infrastructure. It is specifically designed for bbsink implementations that share buffers with their successor bbsink rather than maintaining separate buffer copies. The function forwards archive content data of a specified length to the next bbsink in the chain.

The function enforces strict buffer sharing requirements through assertions, ensuring that the current bbsink and its successor share the same buffer and buffer length. This design avoids unnecessary data copying and memory allocation when multiple bbsinks in a chain need to process the same archive content data. The function is commonly used by bbsink implementations that perform transformations, filtering, or monitoring operations on the archive data stream without needing to modify the actual data.

## Parameters / Member Variables
- `sink`: Pointer to the bbsink structure that is forwarding the archive_contents operation
- `len`: The length of the archive content data to be processed, measured in bytes

## Dependencies
- Functions called/Symbols referenced:
  - bbsink_archive_contents
  - bbsink (type reference)
- Called from (representative examples):
  - [bbsink_progress_archive_contents](bbsink_progress_archive_contents.md) (src/backend/backup/basebackup_progress.c:164)
  - [bbsink_server_archive_contents](bbsink_server_archive_contents.md) (src/backend/backup/basebackup_server.c:187)
  - [bbsink_throttle_archive_contents](bbsink_throttle_archive_contents.md) (src/backend/backup/basebackup_throttle.c:114)

## Notes and Other Information
- This function should only be used when the bbsink shares its buffer with the successor bbsink
- The function performs multiple assertions to ensure proper buffer sharing: bbs_next exists, buffers are identical, and buffer lengths match
- Code using this function should initialize its own bbs_buffer and bbs_buffer_length fields to match the successor sink's values
- The design explicitly avoids data copying to prevent unnecessary memory allocation and improve performance
- This forwarding pattern is essential for maintaining efficiency in multi-stage backup processing pipelines