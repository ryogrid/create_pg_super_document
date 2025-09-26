# bbsink_lz4_archive_contents

## Location
[src/backend/backup/basebackup_lz4.c:180-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_lz4.c#L180-L227)

## Overview
Compresses input data using LZ4 compression and manages buffer space by forwarding compressed data to the next sink when output buffer space becomes insufficient.

## Definition
```c
static void bbsink_lz4_archive_contents(bbsink *sink, size_t avail_in)
```

## Detailed Description
This function performs the core LZ4 compression work for base backup archive contents. It compresses input data from the sink's buffer using `LZ4F_compressUpdate()` and writes the compressed output to the next sink's buffer. The function implements intelligent buffer management - when the remaining space in the output buffer falls below the compression bound for the input data, it invokes the next sink to process accumulated data and resets the bytes_written counter.

The compression is streaming, meaning input data may be compressed across multiple calls without immediately filling the output buffer. This is normal behavior for compression where the compressed size is typically smaller than the input size. The compressed data may be buffered until there's enough data to warrant forwarding to the next sink or until the archive ends.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure (cast to bbsink_lz4 internally)
- `avail_in`: Number of bytes available in the input buffer to compress

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_compressBound (LZ4 library function)
  - LZ4F_compressUpdate (LZ4 library function)
  - LZ4F_isError (LZ4 library function)
  - LZ4F_getErrorName (LZ4 library function)
  - [bbsink_archive_contents](bbsink_archive_contents.md) (calls next sink in chain)
  - elog (error logging)
- Called from (representative examples):
  - Referenced through bbsink_lz4_ops function pointer table

## Notes and Other Information
- Static function, only accessible within the basebackup_lz4.c module
- Implements streaming compression with intelligent buffer management
- May not immediately forward compressed data to avoid buffer overflow
- Uses LZ4F_compressBound() to determine safe compression buffer size
- Maintains bytes_written counter to track output buffer usage
- Part of the sink operation callbacks, called through function pointer indirection
- Error handling includes descriptive LZ4 error messages
- Compressed data output location is calculated as offset from next sink's buffer start