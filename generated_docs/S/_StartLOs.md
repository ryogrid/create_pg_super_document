# _StartLOs

## Location
[src/bin/pg_dump/pg_backup_custom.c:350-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L350-L370)

## Overview
Initializes the format-specific structures and writes control information when beginning to save Binary Large Object (BLOB) data in the custom archive format during a pg_dump operation.

## Definition

```c
static void
_StartLOs(ArchiveHandle *AH, TocEntry *te)
```
## Detailed Description
The  function is a key component of PostgreSQL's custom archive format that handles the initialization phase for Binary Large Object (BLOB) data dumping. This function serves as the counterpart to  but specifically for large object content rather than regular table data.

The function is called by the archiver just prior to the dumper's  routine when preparing to save BLOB DATA (not schema information). It performs several critical setup tasks:

1. **Position Tracking**: Records the current file position to enable efficient seeking during restoration
2. **State Management**: Sets the data state to indicate that the position has been established
3. **Block Identification**: Writes a  marker to identify this section as containing blob data
4. **Sanity Checking**: Writes the dump ID for verification during archive reading

Unlike , this function does not initialize compression, as large object data handling may have different compression requirements managed at a higher level.

## Parameters / Member Variables
- : Archive handle containing the overall archive state and configuration
- : Table of Contents entry representing the specific large object data being processed

## Dependencies
- Functions called/Symbols referenced:
  -  - Gets current position in the archive file
  -  - Writes a single byte to the archive
  -  - Writes an integer value to the archive
- Data structures used:
  -  - Local context for custom format
  -  - Local TOC entry with format-specific data
  -  - General table of contents entry
  -  - Constant indicating position is set
  -  - Block type identifier for blob data blocks
- Called from:
  -  - Custom format initialization
  -  - Directory format initialization
  -  - Null format initialization

## Notes and Other Information
- This function is marked as optional but strongly recommended in the pg_dump architecture
- The  marker distinguishes blob data from regular table data ()
- File position tracking enables efficient random access during archive restoration
- The dumpId serves as a sanity check to ensure archive integrity during reading
- Does not initialize compression, unlike , allowing for different blob handling strategies
- Part of the pluggable archive format system that supports multiple storage backends
- Works in conjunction with  for individual large object processing