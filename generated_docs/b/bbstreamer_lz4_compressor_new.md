# bbstreamer_lz4_compressor_new

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:70-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L70-L115)

## Overview
Creates a new base backup streamer that performs LZ4 compression of tar blocks for PostgreSQL backup operations.

## Definition

```c
bbstreamer *
bbstreamer_lz4_compressor_new(bbstreamer *next, pg_compress_specification *compress)
```
## Detailed Description
This function initializes a new LZ4 compression streamer as part of PostgreSQL's backup streaming system. It sets up the LZ4 compression context, configures compression preferences including block size and compression level, and prepares the streamer for processing backup data. The function only compiles when USE_LZ4 is defined, falling back to a fatal error for builds without LZ4 support.

The streamer uses LZ4 frame format with a maximum 256KB block size and honors the compression level specified in the compress parameter. It initializes internal buffers and sets up the compression context using LZ4F_createCompressionContext.

## Parameters / Member Variables
- `*next`: Pointer to the next streamer in the processing chain
- `*compress`: Compression specification containing level and other compression parameters
## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_lz4_frame](bbstreamer_lz4_frame.md) (struct type)
  - [bbstreamer_ops](bbstreamer_ops.md) (operations table)
  - LZ4F_createCompressionContext
  - [palloc0](../p/palloc0.md)
  - [initStringInfo](../i/initStringInfo.md)
- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (src/bin/pg_basebackup/pg_basebackup.c:1204)

## Notes and Other Information
- Only available when PostgreSQL is built with LZ4 support (USE_LZ4 defined)
- Uses LZ4F_max256KB block size for optimal performance
- Returns NULL and logs fatal error if LZ4 compression context creation fails
- Part of the bbstreamer chain architecture for processing backup data streams