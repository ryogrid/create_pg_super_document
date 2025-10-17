# CreateWalDirectoryMethod

## Location
[src/bin/pg_basebackup/walmethods.c:640-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L640-L690)

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
  - [pg_malloc0](../p/pg_malloc0.md) (PostgreSQL memory allocation)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication)
  - clear_error (internal error clearing function)
- Data structures used:
  - [DirectoryMethodData](../D/DirectoryMethodData.md)
  - [WalWriteMethod](../W/WalWriteMethod.md)
  - [WalWriteMethodOps](../W/WalWriteMethodOps.md)
  - [pg_compress_algorithm](../p/pg_compress_algorithm.md) (enum)
- Called from:
  - [LogStreamerMain](../L/LogStreamerMain.md) (in pg_basebackup.c:568)
  - [StreamLog](../S/StreamLog.md) (in pg_receivewal.c:588)
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

## Simplified Source

```c
WalWriteMethod *
CreateWalDirectoryMethod(const char *basedir,
                         pg_compress_algorithm compression_algorithm,
                         int compression_level, bool sync) {
    DirectoryMethodData *wwmethod;

    // Allocate and zero-initialize the method structure
    wwmethod = pg_malloc0(sizeof(DirectoryMethodData));

    // Set up function pointer table for directory operations
    *((const WalWriteMethodOps **) &wwmethod->base.ops) = &WalDirectoryMethodOps;

    // Configure compression and sync settings
    wwmethod->base.compression_algorithm = compression_algorithm;
    wwmethod->base.compression_level = compression_level;
    wwmethod->base.sync = sync;

    // Initialize error state and store base directory
    clear_error(&wwmethod->base);
    wwmethod->basedir = pg_strdup(basedir);

    return &wwmethod->base;
}
```