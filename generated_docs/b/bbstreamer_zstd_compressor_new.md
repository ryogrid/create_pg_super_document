# bbstreamer_zstd_compressor_new

## Location
[src/bin/pg_basebackup/bbstreamer_zstd.c:66-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_zstd.c#L66-L144)

## Overview
Creates a new base backup streamer that performs Zstandard (zstd) compression of tar blocks in PostgreSQL's backup streaming pipeline.

## Definition

```c
bbstreamer *
bbstreamer_zstd_compressor_new(bbstreamer *next, pg_compress_specification *compress)
```
## Detailed Description
This function initializes a new bbstreamer instance specifically designed for Zstandard compression during PostgreSQL base backups. It creates a compression context using libzstd, configures compression parameters according to the provided specification, and sets up the necessary buffering structures. The streamer follows PostgreSQL's streaming architecture where data flows through a chain of processing nodes, with this compressor fitting into that pipeline.

The function handles various zstd-specific configuration options including compression level, worker thread count for parallel compression, and long-distance matching mode. It initializes the zstd compression context and output buffer, making the streamer ready to process incoming tar data blocks.

## Parameters / Member Variables
- `*next`: The next bbstreamer in the processing chain where compressed data will be forwarded
- `*compress`: A pg_compress_specification structure containing compression configuration options including level, worker count, and feature flags
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
  - ZSTD_createCCtx
  - ZSTD_CCtx_setParameter
  - ZSTD_DStreamOutSize
  - ZSTD_isError
  - ZSTD_getErrorName
  - [pg_fatal](../p/pg_fatal.md)
  - pg_log_error
- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (src/bin/pg_basebackup/pg_basebackup.c:1211)

## Notes and Other Information
- The function is only available when PostgreSQL is compiled with USE_ZSTD support; otherwise it calls pg_fatal
- Sets up compression level, worker threads (if supported by libzstd version), and long-distance matching based on compress specification
- Initializes both the base bbstreamer structure and zstd-specific context and buffers
- Error handling includes checking for zstd context creation failures and parameter setting errors
- The function returns a pointer to the base bbstreamer, allowing it to be used polymorphically in the streaming pipeline

## Simplified Source

```c
bbstreamer *
bbstreamer_zstd_compressor_new(bbstreamer *next, pg_compress_specification *compress)
{
#ifdef USE_ZSTD
    bbstreamer_zstd_frame *streamer;

    // Allocate and initialize the streamer structure
    streamer = palloc0(sizeof(bbstreamer_zstd_frame));
    streamer->base.bbs_ops = &bbstreamer_zstd_compressor_ops;
    streamer->base.bbs_next = next;

    // Initialize buffer for output data
    initStringInfo(&streamer->base.bbs_buffer);
    enlargeStringInfo(&streamer->base.bbs_buffer, ZSTD_DStreamOutSize());

    // Create zstd compression context
    streamer->cctx = ZSTD_createCCtx();
    if (!streamer->cctx)
        pg_fatal("could not create zstd compression context");

    // Set compression level
    if (ZSTD_isError(ZSTD_CCtx_setParameter(streamer->cctx, ZSTD_c_compressionLevel, compress->level)))
        pg_fatal("could not set zstd compression level to %d", compress->level);

    // Set worker count if specified
    if (compress->options & PG_COMPRESSION_OPTION_WORKERS) {
        if (ZSTD_isError(ZSTD_CCtx_setParameter(streamer->cctx, ZSTD_c_nbWorkers, compress->workers)))
            pg_fatal("could not set compression worker count to %d", compress->workers);
    }

    // Enable long-distance matching if requested
    if (compress->options & PG_COMPRESSION_OPTION_LONG_DISTANCE) {
        if (ZSTD_isError(ZSTD_CCtx_setParameter(streamer->cctx, ZSTD_c_enableLongDistanceMatching, compress->long_distance))) {
            pg_log_error("could not enable long-distance mode");
            exit(1);
        }
    }

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