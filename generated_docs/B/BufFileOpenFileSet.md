# BufFileOpenFileSet

## Location
[src/backend/storage/file/buffile.c:291-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L291-L363)

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
- `*fileset`: Pointer to the FileSet containing the BufFile
- `*name`: String identifier of the BufFile to open (same as used in BufFileCreateFileSet)
- `mode`: File access mode (O_RDONLY for read-only, or read-write modes)
- `missing_ok`: If true, returns NULL when BufFile is not found; if false, throws an error
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md): Allocates initial memory for file handle array
  - [repalloc](../r/repalloc.md): Expands file handle array when more segments are discovered
  - [FileSetSegmentName](../F/FileSetSegmentName.md): Constructs segment file names for discovery
  - [FileSetOpen](../F/FileSetOpen.md): Opens individual segment files
  - CHECK_FOR_INTERRUPTS: Allows interruption during potentially long segment discovery
  - [pfree](../p/pfree.md): Frees memory when no segments are found
  - ereport: Reports errors when BufFile cannot be found
  - [makeBufFileCommon](../m/makeBufFileCommon.md): Creates the BufFile structure with discovered segments
  - [pstrdup](../p/pstrdup.md): Duplicates the name string
- Called from (representative examples):
  - [LogicalTapeImport](../L/LogicalTapeImport.md): When importing shared logical tapes for sorting
  - [sts_parallel_scan_next](../s/sts_parallel_scan_next.md): In shared tuple store parallel scanning
  - [apply_spooled_messages](../a/apply_spooled_messages.md): For logical replication message processing
  - [subxact_info_read](../s/subxact_info_read.md): For reading logical replication subxact information

## Notes and Other Information
- Dynamically discovers the number of segments by probing the filesystem sequentially
- Uses exponential growth strategy for the file handle array (doubles capacity when needed)
- The readOnly flag is set based on the mode parameter (true for O_RDONLY)
- Returns NULL if missing_ok is true and no segments are found, otherwise throws ERROR
- Requires the creating backend to have called BufFileClose() or BufFileExportFileSet() first
- Essential for inter-backend file sharing in PostgreSQL's temporary file system
- Uses CHECK_FOR_INTERRUPTS to remain responsive during segment discovery

## Simplified Source

```c
BufFile *
BufFileOpenFileSet(FileSet *fileset, const char *name, int mode,
                   bool missing_ok)
{
    BufFile *file;
    char segment_name[MAXPGPATH];
    Size capacity = 16;
    File *files;
    int nfiles = 0;

    // Start with initial capacity for file handles
    files = palloc(sizeof(File) * capacity);

    // Discover all segments by probing filesystem
    for (;;)
    {
        // Expand array if needed
        if (nfiles + 1 > capacity)
        {
            capacity *= 2;
            files = repalloc(files, sizeof(File) * capacity);
        }

        // Try to open next segment
        FileSetSegmentName(segment_name, name, nfiles);
        files[nfiles] = FileSetOpen(fileset, segment_name, mode);
        if (files[nfiles] <= 0)
            break;  // No more segments
        ++nfiles;

        CHECK_FOR_INTERRUPTS();
    }

    // Handle case where no segments found
    if (nfiles == 0)
    {
        pfree(files);
        if (missing_ok)
            return NULL;

        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open temporary file \"%s\" from BufFile \"%s\": %m",
                        segment_name, name)));
    }

    // Create BufFile with discovered segments
    file = makeBufFileCommon(nfiles);
    file->files = files;
    file->readOnly = (mode == O_RDONLY);
    file->fileset = fileset;
    file->name = pstrdup(name);

    return file;
}
```