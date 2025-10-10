# pgarch_archiveDone

## Location
[src/backend/postmaster/pgarch.c:816-844](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L816-L844)

## Overview
A static function that marks a WAL file as successfully archived by renaming its status file from `.ready` to `.done`, signaling that the archival process is complete.

## Definition
```c
static void pgarch_archiveDone(char *xlog)
```

## Detailed Description
`pgarch_archiveDone` is responsible for completing the archival process workflow by updating the status file associated with a successfully archived WAL file. The function performs a simple but critical file system operation: renaming the status file from `.ready` to `.done`.

This status change serves as a communication mechanism within PostgreSQLs archival system:
- The `.ready` status indicates a file is awaiting archival
- The `.done` status indicates successful archival completion
- Later, checkpoint processes will clean up both the `.done` status file and the corresponding WAL file

The function intentionally uses a non-durable rename operation to avoid extra I/O overhead, relying on the archive commands ability to handle duplicate archival attempts gracefully in case of system crashes.

## Parameters / Member Variables
- `xlog`: A C string containing the name of the WAL file that has been successfully archived

## Dependencies
- Functions called/Symbols referenced:
  - `[StatusFilePath](../S/StatusFilePath.md)`: Constructs file paths for status files (both `.ready` and `.done`)
  - `rename`: Standard C library function for file renaming
  - `ereport`: PostgreSQL error reporting mechanism (for warning on rename failure)
  - [errcode_for_file_access](../e/errcode_for_file_access.md): Provides appropriate error code for file operations
  - [errmsg](../e/errmsg.md): Creates formatted error messages

- Called from (representative examples):
  - [pgarch_ArchiverCopyLoop](pgarch_ArchiverCopyLoop.md): The main archiver loop calls this after successful file archival
  - [arch_files_state](../a/arch_files_state.md): Used in archival file state management

## Notes and Other Information
- This is a static function, only accessible within the pgarch.c source file
- The rename operation is intentionally non-durable to optimize performance
- [Archive](../A/Archive.md) commands must be designed to handle potential re-archival scenarios gracefully
- The function emits warnings but continues execution if the rename fails
- Part of PostgreSQLs WAL archiving infrastructure that ensures reliable backup and recovery capabilities
- The `.done` files serve as markers for the checkpoint process to know when cleanup is safe
- Error handling is non-fatal - failures are logged but dont stop the archival process

## Simplified Source

```c
static void pgarch_archiveDone(char *xlog) {
    char rlogready[MAXPGPATH];
    char rlogdone[MAXPGPATH];

    // Build paths for .ready and .done status files
    StatusFilePath(rlogready, xlog, ".ready");
    StatusFilePath(rlogdone, xlog, ".done");

    // Rename .ready to .done to mark archival complete
    // Non-durable rename for performance - archive commands
    // must handle re-archival gracefully
    if (rename(rlogready, rlogdone) < 0)
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("could not rename file \"%s\" to \"%s\": %m",
                        rlogready, rlogdone)));
}
```