# DirectoryMethodData

## Location
[src/bin/pg_basebackup/walmethods.c:73-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L73-L77)

## Overview
DirectoryMethodData is a structure that represents the directory-based WAL writing method implementation in PostgreSQL's pg_basebackup utility, extending the base WalWriteMethod to store directory-specific data.

## Definition
```c
typedef struct DirectoryMethodData
{
    WalWriteMethod base;
    char       *basedir;
} DirectoryMethodData;
```

## Detailed Description
DirectoryMethodData is a concrete implementation of the WAL writing method for directory-based storage. It embeds the base WalWriteMethod structure as its first member, following PostgreSQL's pattern for method polymorphism. This structure stores the specific data needed for writing WAL files directly to a directory on the filesystem. The structure is used internally by pg_basebackup when performing streaming backup operations where WAL files are written to a specified directory location.

## Parameters / Member Variables
- `base`: The base WalWriteMethod structure containing common method operations, compression settings, synchronization flags, and error handling fields
- `basedir`: A string pointer to the base directory path where WAL files will be written

## Dependencies
- Functions called/Symbols referenced:
  - [WalWriteMethod](../W/WalWriteMethod.md) (base structure)
- Called from (representative examples):
  - [dir_open_for_write](../d/dir_open_for_write.md) (creates and uses DirectoryMethodData instances)
  - [dir_close](../d/dir_close.md) (accesses DirectoryMethodData for file operations)
  - [dir_get_file_size](../d/dir_get_file_size.md) (uses DirectoryMethodData for file size queries)
  - [dir_existsfile](../d/dir_existsfile.md) (checks file existence using DirectoryMethodData)
  - [dir_finish](../d/dir_finish.md) (finalizes DirectoryMethodData operations)
  - [dir_free](../d/dir_free.md) (deallocates DirectoryMethodData)
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (instantiates DirectoryMethodData)

## Notes and Other Information
- This structure follows PostgreSQL's object-oriented programming pattern in C, where the base structure is embedded as the first member
- The basedir field must be properly allocated and managed throughout the lifetime of the DirectoryMethodData instance
- Used specifically in pg_basebackup's streaming backup functionality for directory-based WAL storage
- Part of the pluggable WAL writing method system that also includes tar-based implementations