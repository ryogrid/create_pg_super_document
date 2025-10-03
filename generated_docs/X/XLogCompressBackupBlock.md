# XLogCompressBackupBlock

## Location
[src/backend/access/transam/xloginsert.c:944-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L944-L1026)

## Overview
XLogCompressBackupBlock creates a compressed version of a backup block image for WAL records, supporting multiple compression algorithms to reduce WAL size.

## Definition

```c
static bool
XLogCompressBackupBlock(char *page, uint16 hole_offset, uint16 hole_length,
						char *dest, uint16 *dlen)
```
## Detailed Description
XLogCompressBackupBlock attempts to compress a backup block image using the configured compression algorithm (PGLZ, LZ4, or ZSTD). The function handles pages with holes by first copying the data around the hole into a temporary buffer before compression. It only returns success if the compressed result is actually smaller than the original data, accounting for any extra header bytes needed for compressed blocks with holes. This ensures that compression only occurs when it provides a genuine space benefit.

## Parameters / Member Variables
- `*page`: Pointer to the original page data to be compressed
- `hole_offset`: Byte offset where the hole (unused space) begins in the page
- `hole_length`: Length of the hole in bytes
- `*dest`: Destination buffer to store the compressed data
- `*dlen`: Output parameter set to the length of compressed data on success
## Dependencies
- Functions called/Symbols referenced:
  - [pglz_compress](../p/pglz_compress.md) (for PGLZ compression)
  - LZ4_compress_default (for LZ4 compression)
  - ZSTD_compress (for ZSTD compression)
  - PGAlignedBlock (temporary buffer type)
  - SizeOfXLogRecordBlockCompressHeader (header size constant)
  - COMPRESS_BUFSIZE (compression buffer size)
- Called from:
  - [XLogRecordAssemble](XLogRecordAssemble.md) (during WAL record assembly)

## Notes and Other Information
- Returns false if compression fails or doesn't provide space savings
- Handles pages with holes by copying data around the hole before compression
- Supports multiple compression algorithms based on wal_compression setting
- Only compresses if result + extra header bytes < original size
- LZ4 and ZSTD support depends on build-time configuration

## Simplified Source

```c
// Simplified version of XLogCompressBackupBlock
static bool
XLogCompressBackupBlock(char *page, uint16 hole_offset, uint16 hole_length,
                        char *dest, uint16 *dlen)
{
    int32 original_size = BLCKSZ - hole_length;
    int32 compressed_length = -1;
    int32 header_overhead = 0;
    char *source_data;
    PGAlignedBlock temp_buffer;

    // Handle pages with holes by copying around the hole
    if (hole_length != 0) {
        source_data = temp_buffer.data;
        // Copy data before hole + data after hole into contiguous buffer
        memcpy(source_data, page, hole_offset);
        memcpy(source_data + hole_offset,
               page + (hole_offset + hole_length),
               BLCKSZ - (hole_length + hole_offset));

        // Account for extra header bytes needed for holes
        header_overhead = SizeOfXLogRecordBlockCompressHeader;
    } else {
        source_data = page;  // No hole, use original page directly
    }

    // Apply compression based on configured algorithm
    switch (wal_compression) {
        case WAL_COMPRESSION_PGLZ:
            compressed_length = pglz_compress(source_data, original_size, dest, PGLZ_strategy_default);
            break;

        case WAL_COMPRESSION_LZ4:
            compressed_length = LZ4_compress_default(source_data, dest, original_size, COMPRESS_BUFSIZE);
            if (compressed_length <= 0)
                compressed_length = -1;  // Mark as failure
            break;

        case WAL_COMPRESSION_ZSTD:
            compressed_length = ZSTD_compress(dest, COMPRESS_BUFSIZE, source_data, original_size, ZSTD_CLEVEL_DEFAULT);
            if (ZSTD_isError(compressed_length))
                compressed_length = -1;  // Mark as failure
            break;

        case WAL_COMPRESSION_NONE:
            // Should not happen
            break;
    }

    // Only use compression if it actually saves space
    if (compressed_length >= 0 &&
        compressed_length + header_overhead < original_size) {
        *dlen = (uint16) compressed_length;
        return true;  // Compression successful and beneficial
    }

    return false;  // Compression failed or not beneficial
}
```

Key simplifications made:
- Renamed variables for clarity (orig_len → original_size, len → compressed_length, extra_bytes → header_overhead)
- Added descriptive comments explaining the hole handling logic
- Simplified conditional logic with clearer variable names
- Consolidated error handling patterns across compression algorithms
- Removed build-time conditional compilation directives for readability
- Added comments explaining the space-saving verification logic
- Maintained all essential algorithm steps and return conditions