# bbsink_server

## Location
src/backend/backup/basebackup_server.c: 21 - 34

## Overview
The `bbsink_server` structure is a concrete implementation of the PostgreSQL base backup sink interface designed to store backup archives to server-side directory storage.

## Definition
```c
typedef struct bbsink_server
{
    /* Common information for all types of sink. */
    bbsink      base;

    /* Directory in which backup is to be stored. */
    char       *pathname;

    /* Currently open file (or 0 if nothing open). */
    File        file;

    /* Current file position. */
    off_t       filepos;
} bbsink_server;
```

## Detailed Description
`bbsink_server` is a server-side backup sink that implements the bbsink interface for storing base backup archives directly to the filesystem. It extends the base `bbsink` structure with server-specific functionality for file management and positioning. This sink is responsible for creating and writing backup files to a specified directory path on the PostgreSQL server.

The structure manages the state of file I/O operations during the backup process, tracking the currently open file handle and maintaining the current position within that file. It serves as one of the concrete implementations in the backup sink chain, handling the actual storage of backup data to server-side storage.

## Parameters / Member Variables
- `base`: The common bbsink structure containing callback operations, buffer management, and state information shared across all sink types
- `pathname`: String pointer to the directory path where backup files will be stored on the server filesystem
- `file`: File handle for the currently open backup file (value of 0 indicates no file is currently open)
- `filepos`: Current byte position within the open file, used for tracking write progress and file positioning

## Dependencies
- Functions called/Symbols referenced:
  - bbsink (base structure)
  - File (PostgreSQL file handle type)
- Called from (representative examples):
  - [bbsink_server_new](bbsink_server_new.md)
  - [bbsink_server_begin_archive](bbsink_server_begin_archive.md)
  - [bbsink_server_archive_contents](bbsink_server_archive_contents.md)
  - [bbsink_server_end_archive](bbsink_server_end_archive.md)
  - [bbsink_server_begin_manifest](bbsink_server_begin_manifest.md)
  - [bbsink_server_manifest_contents](bbsink_server_manifest_contents.md)
  - [bbsink_server_end_manifest](bbsink_server_end_manifest.md)

## Notes and Other Information
This structure is part of PostgreSQL's modular backup sink architecture, which allows for chaining multiple processing steps (compression, progress reporting, throttling, etc.) before final storage. The server sink typically appears at the end of the sink chain as the final destination for backup data. The structure is defined in `src/backend/backup/basebackup_server.c` and is created via the `bbsink_server_new()` constructor function, as referenced in the header file `basebackup_sink.h:291`.