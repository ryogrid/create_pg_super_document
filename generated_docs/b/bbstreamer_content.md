# bbstreamer_content

## Location
[src/bin/pg_basebackup/bbstreamer.h:126-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer.h#L126-L135)

## Overview
This function sends content data to a bbstreamer object through its content callback, serving as the primary mechanism for passing archive data through the bbstreamer pipeline.

## Definition

```c
static inline void
bbstreamer_content(bbstreamer *streamer, bbstreamer_member *member,
				   const char *data, int len,
				   bbstreamer_archive_context context)
```
## Detailed Description
bbstreamer_content is a static inline function that provides a standardized interface for sending data chunks to any bbstreamer implementation. It acts as a wrapper around the content callback function pointer in the bbstreamer's operations structure (bbs_ops->content). This function is central to the bbstreamer architecture, enabling data flow through various processing stages such as compression, decompression, archiving, and extraction.

The function performs a basic assertion to ensure the streamer is not NULL before delegating to the appropriate content handler based on the streamer's type. This design allows different bbstreamer implementations to handle content in their own specific ways while maintaining a consistent API.

## Parameters / Member Variables
- `*streamer`: Pointer to the bbstreamer object that will process the content
- `*member`: Pointer to bbstreamer_member struct containing metadata about the current archive member (file path, size, permissions, etc.)
- `*data`: Pointer to the raw data buffer to be processed
- `len`: Length of the data buffer in bytes
- `context`: Enum value indicating the type of data being sent (header, content, trailer, or archive trailer)
## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (struct type)
  - bbstreamer_member (struct type)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (enum type)
  - Assert (assertion macro)

- Called from (representative examples):
  - [bbstreamer_gzip_decompressor_content](bbstreamer_gzip_decompressor_content.md)
  - [bbstreamer_recovery_injector_content](bbstreamer_recovery_injector_content.md)
  - [bbstreamer_lz4_compressor_content](bbstreamer_lz4_compressor_content.md)
  - [bbstreamer_tar_parser_content](bbstreamer_tar_parser_content.md)
  - [bbstreamer_zstd_compressor_content](bbstreamer_zstd_compressor_content.md)
  - [ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md)
  - [ReceiveTarCopyChunk](../R/ReceiveTarCopyChunk.md)

## Notes and Other Information
- This is a static inline function defined in bbstreamer.h, making it available to all bbstreamer implementations
- The function is part of the pg_basebackup utility's streaming backup architecture
- The Assert macro ensures defensive programming by catching NULL streamer pointers in debug builds
- This function is fundamental to the bbstreamer pipeline pattern, where data flows through multiple processing stages
- The context parameter helps streamers understand what type of data they're receiving, enabling appropriate handling for headers, content, and trailers