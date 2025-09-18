# XLogCompressBackupBlock

## Location
[src/backend/access/transam/xloginsert.c:944-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L944-L1026)

## Overview
XLogCompressBackupBlock creates a compressed version of a backup block image for WAL records, supporting multiple compression algorithms to reduce WAL size.

## Definition


## Detailed Description
XLogCompressBackupBlock attempts to compress a backup block image using the configured compression algorithm (PGLZ, LZ4, or ZSTD). The function handles pages with holes by first copying the data around the hole into a temporary buffer before compression. It only returns success if the compressed result is actually smaller than the original data, accounting for any extra header bytes needed for compressed blocks with holes. This ensures that compression only occurs when it provides a genuine space benefit.

## Parameters / Member Variables
- : Pointer to the original page data to be compressed
- : Byte offset where the hole (unused space) begins in the page  
- : Length of the hole in bytes
- : Destination buffer to store the compressed data
- : Output parameter set to the length of compressed data on success

## Dependencies
- Functions called/Symbols referenced:
  - pglz_compress (for PGLZ compression)
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