# bbstreamer_extractor_new

## Location
src/bin/pg_basebackup/bbstreamer_file.c: 183 - 202

## Overview
This function creates a new bbstreamer that extracts files from an archive stream, writing them to the filesystem relative to a specified base path.

## Definition


## Detailed Description
The `bbstreamer_extractor_new` function creates and initializes a new bbstreamer extractor that processes typed archive chunks and extracts files to the filesystem. Unlike plain writers that handle untyped chunks, extractors require properly typed chunks that follow the rules described in bbstreamer.h. The extractor interprets all pathnames in the archive as relative to the provided basepath. It supports callback functions for customizing symbolic link targets and reporting when new output files are opened. The function allocates memory for the extractor structure, initializes the operations vtable pointer, and stores the provided parameters for use during extraction.

## Parameters / Member Variables
- `basepath`: Base directory path where all extracted files will be written relative to this location
- `link_map`: Optional callback function applied to symbolic link targets, returns replacement pathname or NULL to use target unchanged  
- `report_output_file`: Optional callback function called each time a new output file is opened, receives the file pathname as argument or NULL to skip

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (PostgreSQL zero-initialized memory allocation)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - `bbstreamer_extractor_ops` (operations vtable for extractor)
- Called from (representative examples):
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md) at src/bin/pg_basebackup/bbstreamer.h:205
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) at src/bin/pg_basebackup/pg_basebackup.c:1163

## Notes and Other Information
- Returns a pointer to the base bbstreamer structure, allowing polymorphic use
- Does not handle untyped chunks - requires properly structured archive data
- The extractor does not need to worry about original archive format, only member information
- Supports customization through callback functions for link handling and file reporting
- Part of PostgreSQL's backup and restore streaming architecture
- The link_map callback allows for redirecting symbolic links during extraction
- Located in src/bin/pg_basebackup/bbstreamer_file.c:183-202