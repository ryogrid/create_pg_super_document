# CreateWalDirectoryMethod

## Location
src/bin/pg_basebackup/walmethods.c: 640 - 690

## Overview
Creates and initializes a directory-based WAL writing method for PostgreSQL backup operations.

## Definition
```c
WalWriteMethod *CreateWalDirectoryMethod(const char *basedir, 
                                       pg_compress_algorithm compression_algorithm,
                                       int compression_level, bool sync)
```

## Detailed Description
This function is a factory constructor that creates and initializes a DirectoryMethodData structure for handling WAL (Write-Ahead Log) files in a directory-based approach. It allocates memory for the method structure, sets up the function pointers to the directory-specific operations (WalDirectoryMethodOps), configures compression settings, and stores the base directory path. This method is used by pg_basebackup and related tools when WAL files need to be written directly to a filesystem directory rather than being streamed or archived in other formats. The function initializes all necessary fields and returns a WalWriteMethod pointer that can be used for subsequent WAL writing operations.

## Parameters / Member Variables
- `basedir`: The base directory path where WAL files will be written
- `compression_algorithm`: The compression algorithm to use for WAL files
- `compression_level`: The compression level (intensity) to apply
- `sync`: Boolean flag indicating whether to synchronize files to disk

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0 (PostgreSQL memory allocation)
  - pg_strdup (PostgreSQL string duplication)
  - clear_error (internal error clearing function)
- Data structures used:
  - DirectoryMethodData
  - WalWriteMethod
  - WalWriteMethodOps
  - pg_compress_algorithm (enum)
- Called from:
  - LogStreamerMain (in pg_basebackup.c:568)
  - StreamLog (in pg_receivewal.c:588)
  - Referenced in walmethods.h:127

## Notes and Other Information
- Returns a pointer to WalWriteMethod (cast from DirectoryMethodData)
- Allocates memory using pg_malloc0() for zero-initialized allocation  
- Sets up function pointer table (WalDirectoryMethodOps) for directory-specific operations
- Duplicates the basedir string to ensure the method owns its copy
- Initializes compression settings and sync flag according to parameters
- Part of PostgreSQL's pluggable WAL writing method system
- Used primarily by pg_basebackup and pg_receivewal utilities
- The returned pointer should eventually be freed using the corresponding dir_free() function