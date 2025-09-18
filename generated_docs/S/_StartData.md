# _StartData

## Location
src/bin/pg_dump/pg_backup_custom.c: 285 - 311

## Overview
Initializes the format-specific data structures and writes control information when beginning to save table data in the custom archive format during a pg_dump operation.

## Definition


## Detailed Description
The  function is a key component of PostgreSQL's custom archive format implementation in pg_dump. It is called by the archiver just prior to the dumper's  routine being executed when saving TABLE DATA (not schema). This function prepares the archive for receiving table data by:

1. Recording the current file position for potential seeking operations
2. Writing format-specific control markers to the archive stream
3. Initializing compression for the data that will follow

The function operates within the custom archive format context and sets up the necessary state for efficient data writing and later restoration.

## Parameters / Member Variables
- : Archive handle containing the overall archive state and configuration
- : Table of Contents entry representing the specific table data being processed

## Dependencies
- Functions called/Symbols referenced:
  -  - Gets current position in the archive file
  -  - Writes a single byte to the archive
  -  - Writes an integer value to the archive
  -  - Initializes compression for the data stream
- Data structures used:
  -  - Local context for custom format
  -  - Local TOC entry with format-specific data
  -  - General table of contents entry
  -  - Constant indicating position is set
  -  - Block type identifier for data blocks
  -  - Write function for compressed data
- Called from:
  -  - Custom format initialization

## Notes and Other Information
- This function is marked as optional but strongly recommended in the pg_dump architecture
- The function sets up compression using the archive's compression specification
- File position tracking enables efficient seeking during archive restoration
- The BLK_DATA marker and dumpId serve as sanity checks during archive reading
- Part of the pluggable archive format system that allows different storage formats for pg_dump output