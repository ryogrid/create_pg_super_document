# setupRestoreWorker

## Location
src/bin/pg_dump/pg_backup_archiver.c: 210 - 220

## Overview
Sets up a worker process for parallel restore operations by reopening the archive connection.

## Definition
```c
static void setupRestoreWorker(Archive *AHX)
```

## Detailed Description
setupRestoreWorker is a static function designed to initialize worker processes during parallel restore operations. The function performs minimal setup compared to the dump worker counterpart, as noted in the comments: 'The restore worker setup doesn't need to know anything much, so it's defined here.'

The function's primary purpose is to reopen the archive connection for the worker process by calling the ReopenPtr function pointer from the ArchiveHandle structure. This is necessary because worker processes need their own independent connection to the archive being restored.

The function is part of PostgreSQL's parallel restore architecture, where multiple worker processes can restore different parts of an archive simultaneously to improve performance.

## Parameters / Member Variables
- `AHX`: Pointer to the Archive structure (public interface) that will be cast to ArchiveHandle for internal operations

## Dependencies
- Functions called/Symbols referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md): Internal archive structure containing function pointers and state
  - ReopenPtr: Function pointer in ArchiveHandle for reopening archive connections
- Called from (representative examples):
  - [OpenArchive](../O/OpenArchive.md): Uses this function to set up restore workers during archive initialization

## Notes and Other Information
- This is a static function, meaning it's only visible within the pg_backup_archiver.c file
- The function is much simpler than its dump worker counterpart (setupDumpWorker), which is defined in pg_dump.c and requires extensive knowledge of pg_dump internals
- The setup process involves casting the public Archive interface to the internal ArchiveHandle structure
- The ReopenPtr function pointer is format-specific and handles the details of reopening connections for different archive formats
- This function is part of the parallel restore infrastructure that allows multiple workers to restore data concurrently
- The minimal setup reflects that restore workers primarily need to read from archives, which is less complex than the dump process that requires analyzing database schema and dependencies