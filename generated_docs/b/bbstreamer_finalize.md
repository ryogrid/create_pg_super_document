# bbstreamer_finalize

## Location
[src/bin/pg_basebackup/bbstreamer.h:136-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer.h#L136-L143)

## Overview
This function finalizes a bbstreamer object by calling its finalize callback, allowing the streamer to perform cleanup operations and complete any pending work.

## Definition

```c
static inline void
bbstreamer_finalize(bbstreamer *streamer)
```
## Detailed Description
bbstreamer_finalize is a static inline function that provides a standardized interface for finalizing any bbstreamer implementation. It acts as a wrapper around the finalize callback function pointer in the bbstreamer's operations structure (bbs_ops->finalize). This function is called once at the end of data processing to give each bbstreamer in the pipeline a chance to perform cleanup operations such as closing files, flushing buffers, writing final data, or releasing resources.

The function performs a basic assertion to ensure the streamer is not NULL before delegating to the appropriate finalize handler based on the streamer's type. This is part of the three-callback architecture of bbstreamers: content (for processing data), finalize (for cleanup), and free (for memory deallocation).

## Parameters / Member Variables
- `*streamer`: Pointer to the bbstreamer object to be finalized
## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (struct type)
  - Assert (assertion macro)

- Called from (representative examples):
  - [bbstreamer_gzip_decompressor_finalize](bbstreamer_gzip_decompressor_finalize.md)
  - [bbstreamer_recovery_injector_finalize](bbstreamer_recovery_injector_finalize.md)
  - [bbstreamer_lz4_compressor_finalize](bbstreamer_lz4_compressor_finalize.md)
  - [bbstreamer_tar_parser_finalize](bbstreamer_tar_parser_finalize.md)
  - [bbstreamer_zstd_compressor_finalize](bbstreamer_zstd_compressor_finalize.md)
  - [ReceiveArchiveStream](../R/ReceiveArchiveStream.md)
  - [ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md)
  - [ReceiveTarFile](../R/ReceiveTarFile.md)

## Notes and Other Information
- This is a static inline function defined in bbstreamer.h, making it available to all bbstreamer implementations
- Part of the three-phase bbstreamer lifecycle: content processing, finalization, and memory cleanup
- Called exactly once per streamer instance, typically after all content has been processed
- Different bbstreamer types use finalization for various purposes: compression streamers may flush remaining data, file writers may close handles, parsers may process trailing data
- The Assert macro ensures defensive programming by catching NULL streamer pointers in debug builds
- This function is critical for proper resource management in the pg_basebackup streaming architecture

## Simplified Source

```c
static inline void bbstreamer_finalize(bbstreamer *streamer) {
    // Validate streamer exists
    Assert(streamer != NULL);

    // Delegate to streamer-specific finalize handler
    streamer->bbs_ops->finalize(streamer);
}
```