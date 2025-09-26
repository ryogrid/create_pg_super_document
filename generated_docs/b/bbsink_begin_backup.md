# bbsink_begin_backup

## Location
src/include/backup/basebackup_sink.h: 175 - 190

## Overview
Initializes a backup sink by setting up the state and buffer configuration, then calling the sink's begin_backup operation.

## Definition


## Detailed Description
This inline function serves as the initialization entry point for PostgreSQL's base backup sink mechanism. It performs essential setup operations for a backup sink, including state assignment, buffer length configuration, and invoking the sink-specific begin_backup operation. The function includes assertions to ensure proper initialization conditions and validates that the sink's buffer is properly allocated with block-aligned length after the operation completes.

## Parameters / Member Variables
- : Pointer to the backup sink structure that will handle the backup operations
- : Pointer to the backup state structure containing backup session information  
- : Size of the buffer to allocate for backup data transfer (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (structure type)
  - bbsink_state (structure type)
  - Assert (assertion macro)
  - BLCKSZ (block size constant)
- Called from (representative examples):
  - perform_base_backup
  - bbsink_gzip_begin_backup
  - bbsink_lz4_begin_backup
  - bbsink_forward_begin_backup
  - bbsink_zstd_begin_backup

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Includes pre and post-condition assertions for robust error checking
- Ensures buffer length is aligned to PostgreSQL's block size (BLCKSZ) after sink initialization
- The function delegates actual implementation to sink-specific begin_backup operation through function pointer
- Part of PostgreSQL's pluggable backup sink architecture allowing different compression and destination formats