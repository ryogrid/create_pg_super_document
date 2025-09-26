# BasicOpenFilePerm

## Location
src/backend/storage/file/fd.c: 1106 - 1182

## Overview
A robust file opening function that acts as a safe replacement for the standard open() system call, with automatic file descriptor recovery and platform-specific direct I/O handling.

## Definition
```c
int BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
```

## Detailed Description
BasicOpenFilePerm serves as PostgreSQL's primary file opening interface, providing critical enhancements over the standard open() system call:

1. **Automatic FD Recovery**: When open() fails with EMFILE/ENFILE (too many open files), it attempts to free file descriptors by calling ReleaseLruFile() and retries the operation
2. **Direct I/O Support**: Handles platform-specific direct I/O implementation, including F_NOCACHE emulation on systems that don't support O_DIRECT natively
3. **Resource Management**: Integrates with PostgreSQL's file descriptor tracking and management systems

The function includes platform-specific code to handle PG_O_DIRECT flag:
- On systems with native O_DIRECT support, passes it directly to open()
- On macOS and other systems, simulates direct I/O using F_NOCACHE via fcntl()

This function is designed to be the primary (ideally only) direct interface to open() in the PostgreSQL backend, with most code using higher-level VFD (Virtual File Descriptor) abstractions for better resource management.

## Parameters / Member Variables
- `fileName`: Path to the file to be opened
- `fileFlags`: File access flags (O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, etc.) and PostgreSQL-specific flags like PG_O_DIRECT
- `fileMode`: File permissions to use when creating new files (mode_t)

## Dependencies
- Functions called/Symbols referenced:
  - open (system call)
  - close (system call)
  - fcntl (system call, for F_NOCACHE on some platforms)
  - ReleaseLruFile
  - ereport
  - StaticAssertStmt (compile-time assertion)
  - Various O_* constants and PG_O_DIRECT
- Called from (representative examples):
  - BasicOpenFile
  - readRecoverySignalFile
  - LruInsert
  - PathNameOpenFilePerm
  - OpenTransientFilePerm

## Notes and Other Information
- Includes retry logic for EMFILE/ENFILE errors by releasing least-recently-used files
- Handles platform differences for direct I/O through conditional compilation
- Contains compile-time assertions to ensure PG_O_DIRECT doesn't collide with standard flags
- Returns file descriptor on success, -1 on failure (following open() semantics)
- Once a FD is returned, caller is responsible for preventing descriptor leaks on ereport()
- Most PostgreSQL code should use VFD layer instead of calling this directly
- Critical for PostgreSQL's file descriptor resource management strategy
- Logs informational messages when attempting FD recovery