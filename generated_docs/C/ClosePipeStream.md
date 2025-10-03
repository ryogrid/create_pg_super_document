# ClosePipeStream

## Location
[src/backend/storage/file/fd.c:2988-3016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2988-L3016)

## Overview
ClosePipeStream closes a pipe stream that was previously opened by OpenPipeStream and removes it from the internal list of allocated file descriptors managed by PostgreSQL's file descriptor management system.

## Definition

```c
int
ClosePipeStream(FILE *file)
```
## Detailed Description
ClosePipeStream is responsible for properly closing FILE handles that represent pipe streams created through PostgreSQL's managed OpenPipeStream function. The function searches through the internal allocatedDescs array to find the descriptor corresponding to the provided FILE pointer that has the AllocateDescPipe type. If found, it calls FreeDesc to properly clean up the descriptor and close the pipe. If the file was not obtained through OpenPipeStream, it logs a warning and attempts to close the pipe directly using pclose.

This function is part of PostgreSQL's file descriptor management system that ensures proper cleanup of pipe resources. Pipes are commonly used in PostgreSQL for communication with external programs, such as during COPY operations with external commands, SSL certificate operations, and system integration tasks.

## Parameters / Member Variables
- `*file`: The FILE pointer representing a pipe stream to be closed, which should have been obtained from OpenPipeStream
## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - AllocateDesc (descriptor structure type)
  - AllocateDescPipe (enum value for pipe descriptor type)
  - [FreeDesc](../F/FreeDesc.md) (function to free a descriptor)
  - [pclose](../p/pclose.md) (system call to close pipe stream)
  - elog (PostgreSQL logging function)
- Called from (representative examples):
  - [pg_import_system_collations](../p/pg_import_system_collations.md)
  - [ClosePipeFromProgram](ClosePipeFromProgram.md)
  - [ClosePipeToProgram](ClosePipeToProgram.md)
  - [run_ssl_passphrase_command](../r/run_ssl_passphrase_command.md)

## Notes and Other Information
- Returns the result of pclose when closing the pipe directly, or the result of FreeDesc when found in the allocated descriptors
- If a pipe stream not obtained from OpenPipeStream is passed, a WARNING is logged but the function still attempts to close it
- Used specifically for pipe streams created with popen-like functionality through the PostgreSQL file management system
- Part of PostgreSQL's resource management strategy to prevent file descriptor and process leaks
- Commonly used in COPY operations where data is piped to/from external programs
- Essential for proper cleanup of child processes spawned through pipe operations

## Simplified Source

```c
int
ClosePipeStream(FILE *file)
{
    int i;

    // Debug logging for allocated descriptors count
    DO_DB(elog(LOG, "ClosePipeStream: Allocated %d", numAllocatedDescs));

    // Search for the file in allocated descriptors list
    for (i = numAllocatedDescs; --i >= 0;) {
        AllocateDesc *desc = &allocatedDescs[i];

        if (desc->kind == AllocateDescPipe && desc->desc.file == file)
            return FreeDesc(desc);  // Found it, clean it up properly
    }

    // File not found in our managed list - log warning and close directly
    elog(WARNING, "file passed to ClosePipeStream was not obtained from OpenPipeStream");

    return pclose(file);
}
```