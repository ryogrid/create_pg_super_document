# bbsink_archive_contents

## Location
[src/include/backup/basebackup_sink.h:200-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/backup/basebackup_sink.h#L200-L215)

## Overview
Processes and outputs archive data from the sink's buffer by calling the sink-specific archive contents operation.

## Definition

```c
static inline void
bbsink_archive_contents(bbsink *sink, size_t len)
```
## Detailed Description
This inline function handles the processing of archive content data within PostgreSQL's base backup system. It validates that the specified length is within reasonable bounds (non-zero and not exceeding buffer capacity), then delegates to the sink-specific archive_contents operation to handle the actual processing and output of the data. The function expects callers to make reasonable efforts to fill the buffer before invocation, ensuring efficient data processing.

## Parameters / Member Variables
- : Pointer to the backup sink structure that will process the archive contents
- : Number of bytes to process from the sink's buffer (must be > 0 and <= buffer_length)

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (structure type)
  - Assert (assertion macro)
- Called from (representative examples):
  - perform_base_backup
  - sendFileWithContent
  - sendFile
  - push_to_sink
  - _tarWriteHeader
  - _tarWritePadding
  - bbsink_gzip_archive_contents
  - bbsink_lz4_archive_contents
  - bbsink_forward_archive_contents
  - bbsink_zstd_archive_contents

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Includes assertions to validate buffer length constraints for safe operation
- The function delegates actual content processing to sink-specific implementation through function pointer
- Part of PostgreSQL's pluggable backup sink architecture allowing different compression and output formats
- Called frequently during backup operations as data is written to archives
- Expects callers to optimize buffer usage by filling buffers reasonably before calling
- Critical function in the data flow path of base backup operations