# bbstreamer_tar_archiver_new

## Location
src/bin/pg_basebackup/bbstreamer_tar.c: 356 - 389

## Overview
Creates a new bbstreamer that can generate a tar archive, intended for creating brand-new archives or modifying existing ones on the fly.

## Definition


## Detailed Description
This function creates a new tar archiver bbstreamer that can generate properly formatted tar archives. The archiver is designed to be flexible, supporting both the creation of entirely new tar archives and the modification of existing ones during streaming. The input should consist of typed chunks (not BBSTREAMER_UNKNOWN) that represent different parts of the tar archive structure.

The function allocates memory for a  structure and initializes it with the appropriate operations (content, finalize, free) through the  function pointer table. This follows the typical bbstreamer pattern where each streamer type has specific operations for handling data processing, finalization, and cleanup.

## Parameters / Member Variables
- : The next bbstreamer in the processing chain that will receive the processed tar archive data

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - bbstreamer_tar_archiver_ops (operations structure)
  - [bbstreamer](bbstreamer.md) (base streamer type)
  - [bbstreamer_tar_archiver](bbstreamer_tar_archiver.md) (specific archiver structure)
- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (src/bin/pg_basebackup/pg_basebackup.c:1225)
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md) (src/bin/pg_basebackup/bbstreamer.h:218)

## Notes and Other Information
- The archiver maintains state through the  structure, which includes a  boolean flag used to track when tar headers have been regenerated and corresponding padding needs to be updated
- This is part of PostgreSQL's backup streaming infrastructure, used primarily in pg_basebackup operations
- The archiver works in conjunction with other bbstreamer components to form a processing pipeline for backup data