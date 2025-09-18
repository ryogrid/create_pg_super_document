# bbsink_forward_end_archive

## Location
[src/backend/backup/basebackup_sink.c:66-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_sink.c#L66-L75)

## Overview
Forwards the end_archive callback to the next bbsink in a chain, enabling proper finalization of archive processing in a cascaded bbsink architecture.

## Definition
```c
void bbsink_forward_end_archive(bbsink *sink)
```

## Detailed Description
This function implements a forwarding pattern for the end_archive callback within PostgreSQL's base backup sink infrastructure. It is designed to propagate the archive finalization signal through a chain of bbsink implementations. When called, it forwards the end_archive operation to the next bbsink in the chain (sink->bbs_next), ensuring that all bbsinks in the chain can perform their necessary cleanup and finalization operations.

This forwarding mechanism is crucial for maintaining the integrity of multi-stage backup processing pipelines where each bbsink may need to perform specific finalization tasks (such as flushing buffers, closing files, or updating metadata) before the archive processing is considered complete. The function is commonly used by bbsink implementations that perform compression, progress tracking, or other transformations that require cleanup operations.

## Parameters / Member Variables
- `sink`: Pointer to the bbsink structure that is forwarding the end_archive operation to its successor

## Dependencies
- Functions called/Symbols referenced:
  - bbsink_end_archive
  - bbsink (type reference)
- Called from (representative examples):
  - [bbsink_gzip_end_archive](bbsink_gzip_end_archive.md) (src/backend/backup/basebackup_gzip.c:270)
  - [bbsink_lz4_end_archive](bbsink_lz4_end_archive.md) (src/backend/backup/basebackup_lz4.c:266)
  - [bbsink_progress_end_archive](bbsink_progress_end_archive.md) (src/backend/backup/basebackup_progress.c:131)
  - [bbsink_server_end_archive](bbsink_server_end_archive.md) (src/backend/backup/basebackup_server.c:216)
  - [bbsink_zstd_end_archive](bbsink_zstd_end_archive.md) (src/backend/backup/basebackup_zstd.c:275)

## Notes and Other Information
- The function performs an assertion to ensure that bbs_next is properly initialized before forwarding
- This is particularly important for compression-based bbsinks (gzip, lz4, zstd) that need to finalize their compression streams
- The forwarding pattern ensures that all bbsinks in a chain can perform proper cleanup in the correct order
- This function is essential for maintaining the chain of responsibility pattern in composite bbsink architectures where multiple processing stages need coordinated finalization