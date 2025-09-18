# bbsink_lz4_end_archive

## Location
src/backend/backup/basebackup_lz4.c: 228 - 273

## Overview
Finalizes LZ4 compression for an archive by flushing internal buffers, writing the LZ4 footer, and cleaning up compression resources.

## Definition
```c
static void bbsink_lz4_end_archive(bbsink *sink)
```

## Detailed Description
This function performs the finalization tasks required when completing LZ4 compression for an archive. It flushes any remaining data from LZ4's internal buffers using `LZ4F_compressEnd()`, which also writes the LZ4 frame footer. The function manages buffer space by ensuring there's sufficient room for the footer before writing it, potentially flushing accumulated data to the next sink first.

After finalizing compression, it sends all remaining compressed data to the next sink, releases the LZ4 compression context resources, and forwards the end-of-archive notification through the sink chain. The function ensures proper cleanup and resource management while maintaining the integrity of the compressed stream.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure (cast to bbsink_lz4 internally)

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressBound (LZ4 library function)
  - LZ4F_compressEnd (LZ4 library function)
  - LZ4F_isError (LZ4 library function)
  - LZ4F_getErrorName (LZ4 library function)
  - LZ4F_freeCompressionContext (LZ4 library function)
  - bbsink_archive_contents (calls next sink in chain, called twice)
  - bbsink_forward_end_archive (forwards end notification)
  - elog (error logging)
- Called from (representative examples):
  - Referenced through bbsink_lz4_ops function pointer table

## Notes and Other Information
- Static function, only accessible within the basebackup_lz4.c module
- Flushes both internal LZ4 buffers and accumulated output data
- Writes LZ4 frame footer to properly terminate the compressed stream
- Performs buffer space validation before writing footer
- Properly releases LZ4 compression context to prevent memory leaks
- Ensures all compressed data is sent before ending the archive
- Part of the sink operation callbacks, called through function pointer indirection
- Resets bytes_written counter after flushing final data