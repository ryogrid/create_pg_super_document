# bbstreamer_zstd_decompressor_new

## Location
[src/bin/pg_basebackup/bbstreamer_zstd.c:258-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_zstd.c#L258-L295)

## Overview
Creates a new base backup streamer that performs Zstandard (zstd) decompression of compressed tar blocks in PostgreSQL's backup streaming pipeline.

## Definition

```c
bbstreamer *
bbstreamer_zstd_decompressor_new(bbstreamer *next)
```
## Detailed Description
This function initializes a new bbstreamer instance specifically designed for Zstandard decompression during PostgreSQL base backup operations. It creates a decompression context using libzstd and sets up the necessary buffering structures for streaming decompression. The streamer follows PostgreSQL's streaming architecture where compressed data flows through a chain of processing nodes, with this decompressor fitting into that pipeline to restore the original uncompressed data.

The function is simpler than its compression counterpart since decompression doesn't require configuration of compression levels or other parameters - it automatically detects and handles the compression settings from the zstd stream headers.

## Parameters / Member Variables
- `*next`: The next bbstreamer in the processing chain where decompressed data will be forwarded
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
  - ZSTD_createDCtx
  - ZSTD_DStreamOutSize
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (src/bin/pg_basebackup/pg_basebackup.c:1271)

## Notes and Other Information
- The function is only available when PostgreSQL is compiled with USE_ZSTD support; otherwise it calls pg_fatal
- Creates a decompression context (dctx) instead of a compression context (cctx)
- Uses ZSTD_DStreamOutSize() to determine optimal buffer size for decompression output
- Unlike the compressor, no configuration parameters are needed since decompression settings are embedded in the zstd stream
- Initializes the zstd output buffer structure for streaming decompression operations
- The function returns a pointer to the base bbstreamer, allowing polymorphic use in the streaming pipeline
- Error handling includes checking for decompression context creation failures

## Simplified Source

```c
bbstreamer *
bbstreamer_zstd_decompressor_new(bbstreamer *next)
{
#ifdef USE_ZSTD
    bbstreamer_zstd_frame *streamer;

    // Allocate and initialize the streamer structure
    streamer = palloc0(sizeof(bbstreamer_zstd_frame));
    streamer->base.bbs_ops = &bbstreamer_zstd_decompressor_ops;
    streamer->base.bbs_next = next;

    // Initialize buffer for output data
    initStringInfo(&streamer->base.bbs_buffer);
    enlargeStringInfo(&streamer->base.bbs_buffer, ZSTD_DStreamOutSize());

    // Create zstd decompression context
    streamer->dctx = ZSTD_createDCtx();
    if (!streamer->dctx)
        pg_fatal("could not create zstd decompression context");

    // Initialize output buffer for zstd
    streamer->zstd_outBuf.dst = streamer->base.bbs_buffer.data;
    streamer->zstd_outBuf.size = streamer->base.bbs_buffer.maxlen;
    streamer->zstd_outBuf.pos = 0;

    return &streamer->base;
#else
    pg_fatal("this build does not support compression with ZSTD");
    return NULL;
#endif
}
```