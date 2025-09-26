# BufFileCreateFileSet

## Location
src/backend/storage/file/buffile.c: 267 - 290

## Overview
Creates a new BufFile backed by a SharedFileSet that can be discovered and opened read-only by other backends attached to the same FileSet.

## Definition

```c
BufFile *
BufFileCreateFileSet(FileSet *fileset, const char *name)
```
## Detailed Description
BufFileCreateFileSet creates a new BufFile that is backed by a SharedFileSet infrastructure, enabling inter-backend file sharing. Unlike regular BufFiles that are private to a single backend, fileset-based BufFiles can be discovered and opened by other PostgreSQL backends that have access to the same SharedFileSet using the provided name.

The function initializes the BufFile with a single segment (segment 0) and sets it up for read-write operations. The naming scheme is flexible and left to the calling code, with the name appearing as part of filenames on disk. Since each SharedFileSet is backed by uniquely named temporary directories, name conflicts between different SharedFileSet objects are avoided.

## Parameters / Member Variables
- : Pointer to the FileSet that will back this BufFile
- : String identifier for the BufFile that other backends can use to discover it

## Dependencies
- Functions called/Symbols referenced:
  - makeBufFileCommon: Creates basic BufFile structure with specified number of files
  - pstrdup: Duplicates the name string in current memory context
  - palloc: Allocates memory for the file handle array
  - MakeNewFileSetSegment: Creates the initial segment file (segment 0)
- Called from (representative examples):
  - LogicalTapeSetCreate: When creating shared logical tape sets for sorting
  - sts_puttuple: In shared tuple store operations
  - subxact_info_write: For logical replication worker subxact tracking
  - stream_open_file: For logical replication streaming

## Notes and Other Information
- Returns a new BufFile that is initially set to read-write mode (readOnly = false)
- The created BufFile starts with exactly one segment (segment 0)
- Names should be descriptive to help administrators identify which subsystem is generating temporary files
- The SharedFileSet infrastructure ensures proper cleanup and uniqueness across different PostgreSQL processes
- This function is essential for implementing shared temporary storage across multiple backends