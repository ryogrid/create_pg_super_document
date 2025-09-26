# FileSetInit

## Location
[src/backend/storage/file/fileset.c:52-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fileset.c#L52-L91)

## Overview
Initializes a FileSet structure for managing temporary files that can be opened and closed multiple times and survive across transactions, with files distributed across configured tablespaces.

## Definition

```c
void
FileSetInit(FileSet *fileset)
```
## Detailed Description
FileSetInit initializes a FileSet structure that provides a framework for managing temporary files in PostgreSQL. The function sets up the fileset with a unique identifier consisting of the creator's process ID and a counter, ensuring that each fileset is uniquely identifiable across the system. 

The function captures the configured temporary tablespaces from the  GUC parameter, allowing files to be distributed across multiple tablespaces for performance and storage management. If no temporary tablespaces are configured, it defaults to using the current database's default tablespace.

This API is designed for scenarios where temporary files need to:
- Be opened and closed multiple times
- Survive across transaction boundaries  
- Be shared between processes (when used with SharedFileSet)
- Be explicitly managed by the caller (requiring manual cleanup)

## Parameters / Member Variables
- : Pointer to the FileSet structure to initialize

**FileSet Structure Members:**
- : Set to MyProcPid - identifies the creating process
- : Assigned a unique per-PID counter value to distinguish filesets
- : Number of tablespaces available for file distribution
- : Array of tablespace OIDs where files will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PrepareTempTablespaces](../P/PrepareTempTablespaces.md): Prepares the temporary tablespace configuration
  - [GetTempTablespaces](../G/GetTempTablespaces.md): Retrieves the list of configured temporary tablespaces
  - lengthof: Macro to get array length
  - MyProcPid: Current process ID
  - MyDatabaseTableSpace: Default tablespace OID for current database
  - InvalidOid: Constant representing invalid OID

- Called from (representative examples):
  - [stream_start_internal](../s/stream_start_internal.md): Used in logical replication worker
  - [SharedFileSetInit](../S/SharedFileSetInit.md): Used to initialize shared filesets

## Notes and Other Information
- Uses a static counter to ensure unique numbering within each process
- The counter wraps around at INT_MAX to prevent overflow
- Replaces InvalidOid entries in tablespace list with MyDatabaseTableSpace to ensure consistency across all users of the FileSet
- Files created through this API must be explicitly deleted using FileSetDelete/FileSetDeleteAll
- The maximum number of tablespaces is hardcoded to 8, assuming it's rare to have more temporary tablespaces
- This is the foundation for both single-backend temporary file management and shared fileset functionality