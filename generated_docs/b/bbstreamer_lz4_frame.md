# bbstreamer_lz4_frame

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:26-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L26-L36)

## Overview
A specialized structure that extends the base bbstreamer to provide LZ4 frame compression and decompression capabilities for PostgreSQL base backup streaming operations.

## Definition

```c
typedef struct bbstreamer_lz4_frame
{
	bbstreamer	base;

	LZ4F_compressionContext_t cctx;
	LZ4F_decompressionContext_t dctx;
	LZ4F_preferences_t prefs;

	size_t		bytes_written;
	bool		header_written;
} bbstreamer_lz4_frame;
```
## Detailed Description
The  structure is a concrete implementation of the bbstreamer interface specifically designed for LZ4 frame-based compression and decompression operations. It is used in PostgreSQL's pg_basebackup utility to compress or decompress backup data streams using the LZ4 compression algorithm.

This structure inherits from the base  structure, which provides the standard streaming interface with operations for content processing, finalization, and cleanup. The LZ4-specific fields maintain the state needed for frame-based LZ4 operations, including compression/decompression contexts, preferences, and tracking information for the stream processing.

The structure supports both compression and decompression modes, utilizing the LZ4F (LZ4 Frame) API which provides a higher-level interface compared to the block-level LZ4 API. This choice enables better integration with streaming operations and provides features like checksums and block independence.

## Parameters / Member Variables
- : The inherited bbstreamer structure containing the operation function pointers, next streamer in the chain, and a buffer for data accumulation
- : LZ4 frame compression context used when the streamer operates in compression mode
- : LZ4 frame decompression context used when the streamer operates in decompression mode  
- : LZ4 frame preferences structure containing compression settings like block size and compression level
- : Counter tracking the total number of bytes written during the streaming operation
- : Boolean flag indicating whether the LZ4 frame header has been written to the output stream

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - LZ4F_compressionContext_t (from lz4frame.h)
  - LZ4F_decompressionContext_t (from lz4frame.h)
  - LZ4F_preferences_t (from lz4frame.h)

- Called from (representative examples):
  - [bbstreamer_lz4_compressor_new](bbstreamer_lz4_compressor_new.md)
  - [bbstreamer_lz4_compressor_content](bbstreamer_lz4_compressor_content.md)
  - [bbstreamer_lz4_compressor_finalize](bbstreamer_lz4_compressor_finalize.md)
  - [bbstreamer_lz4_compressor_free](bbstreamer_lz4_compressor_free.md)
  - [bbstreamer_lz4_decompressor_new](bbstreamer_lz4_decompressor_new.md)
  - [bbstreamer_lz4_decompressor_content](bbstreamer_lz4_decompressor_content.md)
  - [bbstreamer_lz4_decompressor_finalize](bbstreamer_lz4_decompressor_finalize.md)
  - [bbstreamer_lz4_decompressor_free](bbstreamer_lz4_decompressor_free.md)

## Notes and Other Information
- This structure is only available when PostgreSQL is compiled with LZ4 support (USE_LZ4 preprocessor flag)
- The structure is defined in src/bin/pg_basebackup/bbstreamer_lz4.c:26-36
- It utilizes the LZ4 Frame format rather than the basic LZ4 block format, providing better streaming capabilities
- The compression and decompression contexts are mutually exclusive - a single instance is used for either compression OR decompression, not both
- Memory allocation for instances is handled through PostgreSQL's palloc0() function
- The structure follows PostgreSQL's bbstreamer pattern where the first member must be the base bbstreamer structure
- LZ4 frame preferences can be configured to set compression levels and block sizes (typically set to LZ4F_max256KB)
- Proper error handling is implemented using LZ4F_isError() and LZ4F_getErrorName() functions