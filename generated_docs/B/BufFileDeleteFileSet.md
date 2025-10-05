# BufFileDeleteFileSet

## Location
[src/backend/storage/file/buffile.c:364-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L364-L393)

## Overview
Deletes all segments of a BufFile that was created with BufFileCreateFileSet, providing proactive cleanup rather than waiting for FileSet cleanup.

## Definition

```c
void
BufFileDeleteFileSet(FileSet *fileset, const char *name, bool missing_ok)
```
## Detailed Description
BufFileDeleteFileSet removes all segments of a BufFile that was previously created using BufFileCreateFileSet within the specified FileSet. The function iteratively deletes segments starting from segment 0 and continuing until no more segments are found, since the total number of segments is not known in advance.

This function provides proactive deletion capability, allowing backends to explicitly clean up BufFiles rather than relying on automatic FileSet cleanup. Only one backend should attempt to delete a given BufFile name, and the caller should know that the BufFile exists and has been properly exported or closed.

## Parameters / Member Variables
- `*fileset`: Pointer to the FileSet containing the BufFile to delete
- `*name`: String identifier of the BufFile to delete (same as used in creation/opening)
- `missing_ok`: If true, silently succeeds even if no segments are found; if false, throws an error when BufFile doesn't exist
## Dependencies
- Functions called/Symbols referenced:
  - [FileSetSegmentName](../F/FileSetSegmentName.md): Constructs segment file names for deletion
  - [FileSetDelete](../F/FileSetDelete.md): Deletes individual segment files (with missing_ok=true for segments)
  - CHECK_FOR_INTERRUPTS: Allows interruption during potentially long deletion process
  - elog: Reports errors when BufFile is not found and missing_ok is false
- Called from (representative examples):
  - [subxact_info_write](../s/subxact_info_write.md): Cleanup during logical replication subxact processing
  - [stream_cleanup_files](../s/stream_cleanup_files.md): File cleanup in logical replication streaming operations

## Notes and Other Information
- Deletion is optional - files are automatically cleaned up when the FileSet is destroyed
- Provides proactive cleanup for better resource management in long-running operations
- Uses iterative approach to discover and delete all segments sequentially
- Only the first segment's absence is considered an error (when missing_ok is false)
- The function expects that only one backend will attempt deletion of a given BufFile name
- Uses CHECK_FOR_INTERRUPTS to remain responsive during deletion of many segments
- Part of PostgreSQL's comprehensive temporary file management system for inter-backend file sharing

## Simplified Source
```c
void BufFileDeleteFileSet(FileSet *fileset, const char *name, bool missing_ok) {
    char segment_name[MAXPGPATH];
    int segment = 0;
    bool found = false;

    // Delete all segments until none remain
    for (;;) {
        // Generate segment name and attempt deletion
        FileSetSegmentName(segment_name, name, segment);
        if (!FileSetDelete(fileset, segment_name, true))
            break;  // No more segments to delete

        found = true;
        segment++;
        CHECK_FOR_INTERRUPTS();
    }

    // Report error if no segments found and missing_ok is false
    if (!found && !missing_ok)
        elog(ERROR, "could not delete unknown BufFile \"%s\"", name);
}
```