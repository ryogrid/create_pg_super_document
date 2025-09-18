# bbstreamer_free

## Location
[src/bin/pg_basebackup/bbstreamer.h:144-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer.h#L144-L156)

## Overview
This function frees a bbstreamer object by calling its free callback, allowing the streamer to release allocated memory and resources.

## Definition


## Detailed Description
bbstreamer_free is a static inline function that provides a standardized interface for deallocating any bbstreamer implementation. It acts as a wrapper around the free callback function pointer in the bbstreamer's operations structure (bbs_ops->free). This function is the final step in the bbstreamer lifecycle and is responsible for releasing all memory and resources associated with a bbstreamer instance.

The function performs a basic assertion to ensure the streamer is not NULL before delegating to the appropriate free handler based on the streamer's type. This is crucial in the frontend environment where PostgreSQL's memory contexts are not available, requiring explicit memory management. The free callback typically deallocates the bbstreamer structure itself along with any private data it may contain.

## Parameters / Member Variables
- : Pointer to the bbstreamer object to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (struct type)
  - Assert (assertion macro)

- Called from (representative examples):
  - [bbstreamer_gzip_decompressor_free](bbstreamer_gzip_decompressor_free.md)
  - [bbstreamer_recovery_injector_free](bbstreamer_recovery_injector_free.md)
  - [bbstreamer_lz4_compressor_free](bbstreamer_lz4_compressor_free.md)
  - [bbstreamer_tar_parser_free](bbstreamer_tar_parser_free.md)
  - [bbstreamer_zstd_compressor_free](bbstreamer_zstd_compressor_free.md)
  - [ReceiveArchiveStream](../R/ReceiveArchiveStream.md)
  - [ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md)
  - [ReceiveTarFile](../R/ReceiveTarFile.md)

## Notes and Other Information
- This is a static inline function defined in bbstreamer.h, making it available to all bbstreamer implementations
- Final phase of the three-callback bbstreamer lifecycle: content processing, finalization, and memory cleanup
- Called exactly once per streamer instance, after finalization is complete
- Critical for preventing memory leaks in the frontend environment where automatic memory management is not available
- Different bbstreamer implementations free various resources: file handles, compression contexts, buffers, and the streamer structure itself
- The Assert macro ensures defensive programming by catching NULL streamer pointers in debug builds
- Should be called on all bbstreamer instances to ensure proper cleanup in the pg_basebackup utility