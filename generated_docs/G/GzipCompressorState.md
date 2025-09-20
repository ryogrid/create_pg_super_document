# GzipCompressorState

## Location
[src/bin/pg_dump/compress_gzip.c:27-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L27-L33)

## Overview
GzipCompressorState is a structure that maintains the state for gzip compression operations in PostgreSQL's pg_dump utility, encapsulating the zlib stream and output buffer management.

## Definition

```c
typedef struct GzipCompressorState
{
	z_streamp	zp;

	void	   *outbuf;
	size_t		outsize;
} GzipCompressorState;
```
## Detailed Description
GzipCompressorState serves as the private state container for gzip compression functionality within PostgreSQL's backup and restore system. It is part of the compressor API abstraction layer that allows pg_dump to support multiple compression formats. This structure maintains the zlib compression stream and manages the output buffer used during compression operations.

The structure is designed to work with the zlib library (when HAVE_LIBZ is defined) and provides a clean interface for deflate compression operations. It is allocated and managed by the deflate compressor functions and stored in the CompressorState's private_data field.

## Parameters / Member Variables
- `zp`: Pointer to the zlib z_stream structure that maintains the compression state, handles input/output buffers, and tracks compression progress
- `*outbuf`: Output buffer that stores compressed data before it is written to the archive
- `outsize`: Size of the output buffer, typically set to DEFAULT_IO_BUFFER_SIZE with one extra byte allocated for potential trailing zero
## Dependencies
- Functions called/Symbols referenced:
  - z_streamp (from zlib library)
- Called from (representative examples):
  - [DeflateCompressorInit](../D/DeflateCompressorInit.md) (initializes the structure)
  - [DeflateCompressorEnd](../D/DeflateCompressorEnd.md) (cleans up the structure)
  - [DeflateCompressorCommon](../D/DeflateCompressorCommon.md) (uses the structure during compression)
  - [WriteDataToArchiveGzip](../W/WriteDataToArchiveGzip.md) (accesses the structure for data compression)

## Notes and Other Information
- This structure is only available when PostgreSQL is compiled with zlib support (HAVE_LIBZ)
- The output buffer is allocated with outsize + 1 bytes to accommodate routines that append trailing zero bytes
- Memory management is handled by DeflateCompressorInit (allocation) and DeflateCompressorEnd (deallocation)
- The structure is accessed through type casting from CompressorState's private_data field
- Part of the broader compression abstraction layer in pg_dump that supports multiple compression formats
- Located in src/bin/pg_dump/compress_gzip.c:27-33