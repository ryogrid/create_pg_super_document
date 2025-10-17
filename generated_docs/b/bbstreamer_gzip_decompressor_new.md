# bbstreamer_gzip_decompressor_new

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:212-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L212-L260)

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
  - [bbstreamer](bbstreamer.md) (return type)
  - [bbstreamer_gzip_decompressor](bbstreamer_gzip_decompressor.md) (internal structure type)
  - [bbstreamer_ops](bbstreamer_ops.md) (operations structure)
  - [gzip_palloc](../g/gzip_palloc.md) (custom memory allocator)
  - [gzip_pfree](../g/gzip_pfree.md) (custom memory deallocator)
- Called from (representative examples):
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md) (in bbstreamer.h:209)
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (in pg_basebackup.c:1267)

## Notes and Other Information
- Requires HAVE_LIBZ to be defined, otherwise raises a fatal error
- Uses inflateInit2 with windowBits=31 (15+16) for maximum compatibility and gzip header support
- Part of the PostgreSQL base backup streaming infrastructure
- Memory allocation for zlib operations is handled through PostgreSQL's memory context system via custom allocators
- The decompressor maintains its own buffer for accumulating decompressed data before passing it to the next streamer in the chain

## Simplified Source

```c
bbstreamer *
bbstreamer_gzip_decompressor_new(bbstreamer *next)
{
#ifdef HAVE_LIBZ
    bbstreamer_gzip_decompressor *streamer;
    z_stream *zs;

    Assert(next != NULL);

    // Allocate and initialize decompressor structure
    streamer = palloc0(sizeof(bbstreamer_gzip_decompressor));
    *((const bbstreamer_ops **) &streamer->base.bbs_ops) = &bbstreamer_gzip_decompressor_ops;

    // Set up streaming chain and buffer
    streamer->base.bbs_next = next;
    initStringInfo(&streamer->base.bbs_buffer);

    // Initialize zlib decompression stream
    zs = &streamer->zstream;
    zs->zalloc = gzip_palloc;
    zs->zfree = gzip_pfree;
    zs->next_out = (uint8 *) streamer->base.bbs_buffer.data;
    zs->avail_out = streamer->base.bbs_buffer.maxlen;

    // Initialize decompression with gzip header support (15 + 16)
    if (inflateInit2(zs, 15 + 16) != Z_OK)
        pg_fatal("could not initialize compression library");

    return &streamer->base;
#else
    pg_fatal("this build does not support compression with %s", "gzip");
    return NULL;
#endif
}
```