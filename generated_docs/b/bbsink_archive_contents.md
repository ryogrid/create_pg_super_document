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
- `*sink`: Pointer to the backup sink structure that will process the archive contents
- `len`: Number of bytes to process from the sink's buffer (must be > 0 and <= buffer_length)
## Dependencies
- Functions called/Symbols referenced:
  - [bbsink](bbsink.md) (structure type)
  - Assert (assertion macro)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [sendFileWithContent](../s/sendFileWithContent.md)
  - [sendFile](../s/sendFile.md)
  - [push_to_sink](../p/push_to_sink.md)
  - [_tarWriteHeader](../t/_tarWriteHeader.md)
  - [_tarWritePadding](../t/_tarWritePadding.md)
  - [bbsink_gzip_archive_contents](bbsink_gzip_archive_contents.md)
  - [bbsink_lz4_archive_contents](bbsink_lz4_archive_contents.md)
  - [bbsink_forward_archive_contents](bbsink_forward_archive_contents.md)
  - [bbsink_zstd_archive_contents](bbsink_zstd_archive_contents.md)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Includes assertions to validate buffer length constraints for safe operation
- The function delegates actual content processing to sink-specific implementation through function pointer
- Part of PostgreSQL's pluggable backup sink architecture allowing different compression and output formats
- Called frequently during backup operations as data is written to archives
- Expects callers to optimize buffer usage by filling buffers reasonably before calling
- Critical function in the data flow path of base backup operations

## Simplified Source

```c
// Simplified version of bbsink_archive_contents
static inline void bbsink_archive_contents(bbsink *sink, size_t len) {
    Assert(sink != NULL);
    Assert(len > 0 && len <= sink->bbs_buffer_length);

    // Delegate to sink-specific implementation
    sink->bbs_ops->archive_contents(sink, len);
}
```

Key simplifications made:
- Removed detailed comments while preserving validation logic
- Maintained essential assertions for buffer length validation
- Preserved the delegation pattern to sink-specific operations
- Kept the inline function optimization
- Maintained defensive programming with null pointer checks