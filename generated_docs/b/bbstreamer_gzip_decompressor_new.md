# bbstreamer_gzip_decompressor_new

## Location
src/bin/pg_basebackup/bbstreamer_gzip.c: 212 - 260

## Overview
Creates a new base backup streamer that performs decompression of gzip compressed blocks, providing a streaming decompression interface for PostgreSQL backup data.

## Definition
```c
bbstreamer *bbstreamer_gzip_decompressor_new(bbstreamer *next)
```

## Detailed Description
This function creates and initializes a new bbstreamer that decompresses gzip-compressed data in a streaming fashion. It sets up the necessary internal state including a z_stream structure for zlib decompression operations. The decompressor is configured to handle gzip headers by using inflateInit2 with windowBits parameter set to 15 + 16 (maximum window size plus gzip header detection). The function also configures custom memory allocation functions (gzip_palloc/gzip_pfree) for the zlib operations and initializes the output buffer. This streamer is designed to be chained with other streamers in a pipeline architecture.

## Parameters / Member Variables
- `next`: The next bbstreamer in the processing chain that will receive the decompressed data

## Dependencies
- Functions called/Symbols referenced:
  - bbstreamer (return type)
  - bbstreamer_gzip_decompressor (internal structure type)
  - bbstreamer_ops (operations structure)
  - gzip_palloc (custom memory allocator)
  - gzip_pfree (custom memory deallocator)
- Called from (representative examples):
  - bbstreamer_buffer_until (in bbstreamer.h:209)
  - CreateBackupStreamer (in pg_basebackup.c:1267)

## Notes and Other Information
- Requires HAVE_LIBZ to be defined, otherwise raises a fatal error
- Uses inflateInit2 with windowBits=31 (15+16) for maximum compatibility and gzip header support
- Part of the PostgreSQL base backup streaming infrastructure
- Memory allocation for zlib operations is handled through PostgreSQL's memory context system via custom allocators
- The decompressor maintains its own buffer for accumulating decompressed data before passing it to the next streamer in the chain