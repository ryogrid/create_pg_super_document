# dirent

## Location
src/include/port/win32_msvc/dirent.h: 9 - 17

## Overview
The  structure represents a directory entry in PostgreSQL's Windows-specific directory traversal implementation, providing a portable interface for directory operations on Windows platforms.

## Definition


## Detailed Description
The  structure is part of PostgreSQL's Windows compatibility layer, specifically designed for MSVC builds on Windows. This structure emulates the POSIX  interface that is natively available on Unix-like systems but absent on Windows. It serves as a bridge between Windows file system APIs and PostgreSQL's cross-platform directory traversal code.

The structure is used in conjunction with , , and  functions to provide a POSIX-like directory scanning interface. When  is called, it returns a pointer to a  structure containing information about the current directory entry being processed.

The implementation handles Windows-specific file attributes and maps them to POSIX-style file types, including special handling for reparse points (symbolic links) and proper classification of directories vs. regular files.

## Parameters / Member Variables
- : Inode number (always set to 0 on Windows as the concept doesn't directly apply)
- : Record length (not used on Windows, set to 0)
- : File type indicator using DT_* constants (DT_DIR, DT_REG, DT_LNK, etc.)
- : Length of the filename in 
- : Null-terminated filename, with maximum length of MAX_PATH characters

## Dependencies
- Functions called/Symbols referenced:
  - MAX_PATH (Windows constant)
  - DT_* constants (DT_DIR, DT_REG, DT_LNK, DT_UNKNOWN, etc.)
- Called from (representative examples):
  - CheckPointLogicalRewriteHeap
  - [SlruScanDirectory](../S/SlruScanDirectory.md) 
  - [RemoveOldXlogFiles](../R/RemoveOldXlogFiles.md)
  - [sendDir](../s/sendDir.md)
  - copydir
  - AllocateDir
  - ReadDir
  - [pg_ls_dir](../p/pg_ls_dir.md)
  - [walkdir](../w/walkdir.md)
  - rmtree

## Notes and Other Information
- This is a Windows-specific implementation found in 
- The structure is part of PostgreSQL's portability layer, ensuring consistent directory operations across platforms
- File type detection is performed by examining Windows file attributes and mapping them to POSIX equivalents
- Special handling exists for Windows reparse points, which are treated as symbolic links (DT_LNK)
- The implementation is used extensively throughout PostgreSQL for file system operations including WAL file management, backup operations, extension loading, and general directory traversal
- The  and  fields are included for POSIX compatibility but are not meaningful on Windows