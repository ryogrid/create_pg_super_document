# BufFileOpenFileSet

## Location
src/backend/storage/file/buffile.c: 291 - 363

## Overview
Opens an existing BufFile that was previously created with BufFileCreateFileSet in the same FileSet, discovering all segments and preparing it for read-only or read-write access.

## Definition

```c
BufFile *
BufFileOpenFileSet(FileSet *fileset, const char *name, int mode,
				   bool missing_ok)
```
## Detailed Description
BufFileOpenFileSet opens a multi-segment BufFile that was previously created by another backend (or the same backend) using BufFileCreateFileSet. The function dynamically discovers all segments of the BufFile by probing the filesystem, since the number of segments is not known in advance.

The function starts with an initial capacity for file handles and expands it as needed while discovering segments. It attempts to open each segment in sequence (segment 0, 1, 2, ...) until it encounters a missing segment, which indicates the end of the file set. The creating backend must have properly closed or exported the BufFile before it can be opened by other backends.

## Parameters / Member Variables
- : Pointer to the FileSet containing the BufFile
- : String identifier of the BufFile to open (same as used in BufFileCreateFileSet)
- : File access mode (O_RDONLY for read-only, or read-write modes)
- : If true, returns NULL when BufFile is not found; if false, throws an error

## Dependencies
- Functions called/Symbols referenced:
  - palloc: Allocates initial memory for file handle array
  - repalloc: Expands file handle array when more segments are discovered
  - FileSetSegmentName: Constructs segment file names for discovery
  - FileSetOpen: Opens individual segment files
  - CHECK_FOR_INTERRUPTS: Allows interruption during potentially long segment discovery
  - pfree: Frees memory when no segments are found
  - ereport: Reports errors when BufFile cannot be found
  - makeBufFileCommon: Creates the BufFile structure with discovered segments
  - pstrdup: Duplicates the name string
- Called from (representative examples):
  - LogicalTapeImport: When importing shared logical tapes for sorting
  - sts_parallel_scan_next: In shared tuple store parallel scanning
  - apply_spooled_messages: For logical replication message processing
  - subxact_info_read: For reading logical replication subxact information

## Notes and Other Information
- Dynamically discovers the number of segments by probing the filesystem sequentially
- Uses exponential growth strategy for the file handle array (doubles capacity when needed)
- The readOnly flag is set based on the mode parameter (true for O_RDONLY)
- Returns NULL if missing_ok is true and no segments are found, otherwise throws ERROR
- Requires the creating backend to have called BufFileClose() or BufFileExportFileSet() first
- Essential for inter-backend file sharing in PostgreSQL's temporary file system
- Uses CHECK_FOR_INTERRUPTS to remain responsive during segment discovery