# bbstreamer

## Location
[src/bin/pg_basebackup/bbstreamer.h:31-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer.h#L31-L31)

## Overview
The bbstreamer structure is the core component of PostgreSQL's backup streaming architecture, used to process tar archive data received from the server through a pipeline of transformations such as compression, decompression, parsing, and file extraction.

## Definition

```c
typedef struct bbstreamer bbstreamer;
```
## Detailed Description
The bbstreamer represents a single stage in a processing pipeline for backup data streams. Each tar archive returned by the PostgreSQL server is passed through one or more bbstreamer objects for processing. These objects can perform various operations ranging from simple tasks like writing archives to files (possibly with compression) to complex operations like parsing the byte stream to annotate different parts of the data (tar headers, payload data, trailing padding) or modifying archive contents.

The bbstreamer architecture follows a chain-of-responsibility pattern where each bbstreamer can optionally forward processed data to a successor bbstreamer, enabling complex processing pipelines. The system is designed to handle streaming data efficiently, allowing for real-time processing of backup data as it arrives from the server.

## Parameters / Member Variables
- : Pointer to the bbstreamer_ops structure containing function pointers for content processing, finalization, and cleanup operations specific to this bbstreamer type
- : Pointer to the next bbstreamer in the processing chain; set to NULL when this is the final stage in the pipeline
- : StringInfoData buffer used for accumulating data for temporary storage; each bbstreamer type decides how to use this buffer

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_ops](bbstreamer_ops.md) (for operation function pointers)
  - [StringInfoData](../S/StringInfoData.md) (for internal buffering)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (for data classification)
  - bbstreamer_member (for archive member metadata)

- Called from (representative examples):
  - [bbstreamer_plain_writer_new](bbstreamer_plain_writer_new.md)
  - [bbstreamer_extractor_new](bbstreamer_extractor_new.md)
  - [bbstreamer_gzip_writer_new](bbstreamer_gzip_writer_new.md)
  - [bbstreamer_tar_parser_new](bbstreamer_tar_parser_new.md)
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)
  - [ReceiveTarFile](../R/ReceiveTarFile.md)

## Notes and Other Information
- The bbstreamer system is designed for frontend environments where memory contexts are not available, requiring explicit memory management through the 'free' callback
- All bbstreamer operations should be invoked through the provided static inline functions (bbstreamer_content, bbstreamer_finalize, bbstreamer_free) rather than calling the function pointers directly
- The first element of any bbstreamer-derived structure should be 'bbstreamer base' to maintain compatibility with the polymorphic interface
- The bbs_buffer is a general-purpose buffer that different bbstreamer implementations can use for various purposes like accumulating partial data, buffering for compression, or storing temporary transformation results