# PathNameOpenFilePerm

## Location
src/backend/storage/file/fd.c: 1585 - 1656

## Overview
PathNameOpenFilePerm opens a file in an arbitrary directory with explicit file permission control, serving as the core file opening function in PostgreSQL's virtual file descriptor (VFD) system.

## Definition


## Detailed Description
PathNameOpenFilePerm is the primary function for opening files within PostgreSQL's virtual file descriptor management system. It creates a VFD entry for the specified file, manages kernel file descriptor limits through the LRU cache system, and ensures proper cleanup on failure. The function automatically adds O_CLOEXEC to prevent file descriptor inheritance by child processes, and handles memory management for the filename copy. If the pathname is relative, it's interpreted relative to the process working directory (typically $PGDATA). The function integrates with PostgreSQL's resource management by tracking the VFD in the cache and managing kernel FD limits.

## Parameters / Member Variables
- : Path to the file to be opened (relative paths interpreted from $PGDATA)
- : File access flags for opening (read, write, create, etc.)
- : File permissions to use when creating new files

## Dependencies
- Functions called/Symbols referenced:
  - AllocateVfd
  - ReleaseLruFiles  
  - BasicOpenFilePerm
  - FreeVfd
  - Insert
  - strdup
  - ereport
- Called from (representative examples):
  - PathNameOpenFile

## Notes and Other Information
This function is central to PostgreSQL's file management architecture, located in src/backend/storage/file/fd.c. It implements a virtual file descriptor system that allows PostgreSQL to manage more files than the kernel limit permits by caching and reusing kernel FDs. The function ensures proper error handling and cleanup, including freeing allocated memory on failure. All descriptors are implicitly marked O_CLOEXEC for security. The saved flags are adjusted (removing O_CREAT, O_TRUNC, O_EXCL) to allow safe re-opening of the file later.