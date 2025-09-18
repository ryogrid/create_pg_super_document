# FileSet

## Location
src/include/storage/fileset.h: 22 - 30

## Overview
FileSet is a structure that represents a set of temporary files that can be distributed across multiple tablespaces and shared between processes or used within a single backend across transactions.

## Definition


## Detailed Description
FileSet provides a mechanism for managing temporary files that need to persist across transactions or be shared between multiple processes. Unlike regular temporary files that are automatically cleaned up, FileSet files require explicit cleanup using FileSetDelete or FileSetDeleteAll functions.

The structure supports distributing files across multiple tablespaces as configured in the temp_tablespaces GUC parameter. This allows for better I/O distribution and storage management when dealing with large temporary datasets.

Each FileSet is uniquely identified by the combination of creator_pid and number, ensuring that different processes can create their own FileSets without conflicts. The files in a FileSet are organized in directories that correspond to the FileSet identifier.

## Parameters / Member Variables
- : The process ID of the process that created this FileSet, used for unique identification across the system
- : A per-process identifier that, combined with creator_pid, uniquely identifies this FileSet
- : The number of tablespaces available for storing files in this FileSet (limited to 8)
- : Array of tablespace OIDs where files can be created, with a maximum of 8 tablespaces supported

## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - Oid (Object Identifier type)
- Called from (representative examples):
  - FileSetInit
  - FileSetCreate
  - FileSetOpen
  - FileSetDelete
  - FileSetDeleteAll
  - BufFileCreateFileSet
  - BufFileOpenFileSet
  - BufFileDeleteFileSet
  - LogicalRepWorker (in replication workers)
  - ParallelApplyWorkerShared (in parallel apply workers)

## Notes and Other Information
- The structure assumes it's rare to have more than 8 temporary tablespaces, which is why the tablespaces array is fixed at size 8
- FileSet is designed for scenarios where temporary files need to survive across transactions or be accessed by multiple processes
- Used extensively in logical replication workers for managing temporary data during replication operations
- Integrates with PostgreSQL's buffer file (BufFile) system for efficient I/O operations
- Files in a FileSet must be explicitly deleted by the application - they are not automatically cleaned up like regular temporary files
- The temp_tablespaces GUC parameter controls which tablespaces are used for file distribution