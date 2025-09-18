# bbstreamer_ops

## Location
src/bin/pg_basebackup/bbstreamer.h: 32 - 54

## Overview
The bbstreamer_ops structure defines the operation interface for bbstreamer objects, containing function pointers that implement the specific behavior for each type of backup stream processor.

## Definition


## Detailed Description
The bbstreamer_ops structure implements a function pointer interface that enables polymorphic behavior for different types of bbstreamer objects. This design allows the same bbstreamer interface to support various implementations such as file writers, compressors, decompressors, tar parsers, and archive extractors. Each bbstreamer type provides its own implementation of these three core operations.

The interface follows a three-phase lifecycle: content processing (called repeatedly with data chunks), finalization (called once at the end for cleanup), and resource deallocation (called to free memory). This design is particularly important in frontend environments where memory contexts are not available, requiring explicit resource management.

## Parameters / Member Variables
- : Function pointer for processing data chunks; called repeatedly with archive data, member metadata, and context information about the data type (header, contents, trailer, etc.)
- : Function pointer for end-of-stream processing; called once when all data has been processed to perform cleanup operations like closing files or flushing buffers
-                total        used        free      shared  buff/cache   available
Mem:        32819380     7700764    19738404        3072     5380212    24736432
Swap:        8388608           0     8388608: Function pointer for resource deallocation; called to release memory and other resources allocated by the bbstreamer implementation

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (main structure that contains this ops pointer)
  - bbstreamer_member (for archive member metadata)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (for data type classification)

- Called from (representative examples):
  - [bbstreamer_content](bbstreamer_content.md) (static inline wrapper)
  - [bbstreamer_finalize](bbstreamer_finalize.md) (static inline wrapper)
  - [bbstreamer_free](bbstreamer_free.md) (static inline wrapper)
  - [bbstreamer_plain_writer_new](bbstreamer_plain_writer_new.md)
  - [bbstreamer_gzip_writer_new](bbstreamer_gzip_writer_new.md)
  - [bbstreamer_tar_parser_new](bbstreamer_tar_parser_new.md)

## Notes and Other Information
- The ops structure should always be accessed through the provided static inline wrapper functions (bbstreamer_content, bbstreamer_finalize, bbstreamer_free) rather than calling the function pointers directly
- Each bbstreamer implementation must provide all three function pointers; none should be NULL
- The content function may be called multiple times with different data chunks and contexts, while finalize and free are called exactly once per bbstreamer instance
- The design supports chaining of bbstreamer objects, where one bbstreamer's content function may call another bbstreamer's content function to create processing pipelines
- Memory management is critical since this code runs in frontend environments without memory contexts