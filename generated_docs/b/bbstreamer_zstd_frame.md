# bbstreamer_zstd_frame

## Location
[src/bin/pg_basebackup/bbstreamer_zstd.c:25-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_zstd.c#L25-L32)

## Overview
A specialized bbstreamer structure that extends the base bbstreamer to provide Zstandard (ZSTD) compression and decompression functionality for PostgreSQL base backup streaming operations.

## Definition

```c
typedef struct bbstreamer_zstd_frame
{
	bbstreamer	base;

	ZSTD_CCtx  *cctx;
	ZSTD_DCtx  *dctx;
	ZSTD_outBuffer zstd_outBuf;
} bbstreamer_zstd_frame;
```
## Detailed Description
The bbstreamer_zstd_frame structure is a concrete implementation of the bbstreamer interface specifically designed for handling ZSTD compression and decompression of tar archive data during PostgreSQL base backup operations. It maintains both compression and decompression contexts to allow bidirectional ZSTD operations, along with an output buffer for managing compressed/decompressed data flow.

This structure is part of PostgreSQL's modular streaming architecture where tar archives from the server can be processed through a chain of bbstreamer objects. The ZSTD variant provides high-performance compression capabilities while maintaining the standard bbstreamer interface for seamless integration with other streaming components.

The structure is conditionally compiled only when USE_ZSTD is defined, ensuring it's only available when ZSTD library support is enabled during PostgreSQL compilation.

## Parameters / Member Variables
- : The base bbstreamer structure containing standard operations, next streamer pointer, and buffer management
- : ZSTD compression context pointer used for compressing data streams
- : ZSTD decompression context pointer used for decompressing data streams  
- : ZSTD output buffer structure managing the output data during compression/decompression operations

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - ZSTD_CCtx (ZSTD compression context type)
  - ZSTD_DCtx (ZSTD decompression context type)
  - ZSTD_outBuffer (ZSTD output buffer type)

- Called from (representative examples):
  - [bbstreamer_zstd_compressor_new](bbstreamer_zstd_compressor_new.md)
  - [bbstreamer_zstd_decompressor_new](bbstreamer_zstd_decompressor_new.md)
  - [bbstreamer_zstd_compressor_content](bbstreamer_zstd_compressor_content.md)
  - [bbstreamer_zstd_compressor_finalize](bbstreamer_zstd_compressor_finalize.md)
  - [bbstreamer_zstd_compressor_free](bbstreamer_zstd_compressor_free.md)
  - [bbstreamer_zstd_decompressor_content](bbstreamer_zstd_decompressor_content.md)
  - [bbstreamer_zstd_decompressor_finalize](bbstreamer_zstd_decompressor_finalize.md)
  - [bbstreamer_zstd_decompressor_free](bbstreamer_zstd_decompressor_free.md)

## Notes and Other Information
- This structure is only available when PostgreSQL is compiled with ZSTD support (USE_ZSTD macro defined)
- The structure maintains separate contexts for both compression and decompression, though typically only one would be used depending on the operation mode
- Part of the pg_basebackup utility's streaming infrastructure for handling backup data transformation
- The ZSTD implementation provides high compression ratios with fast compression/decompression speeds
- Memory management follows PostgreSQL's frontend conventions since this runs in a client-side environment without memory contexts
- The structure supports the standard bbstreamer callback interface (content, finalize, free) through function pointers in the base.bbs_ops field