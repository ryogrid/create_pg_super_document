# TarMethodData

## Location
[src/bin/pg_basebackup/walmethods.c:699-709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L699-L709)

## Overview
TarMethodData is a structure that represents the tar-based WAL writing method implementation in PostgreSQL's pg_basebackup utility, extending the base WalWriteMethod to store tar-specific archive management data.

## Definition
```c
typedef struct TarMethodData
{
    WalWriteMethod base;
    char       *tarfilename;
    int         fd;
    TarMethodFile *currentfile;
#ifdef HAVE_LIBZ
    z_streamp   zp;
    void       *zlibOut;
#endif
} TarMethodData;
```

## Detailed Description
TarMethodData is a concrete implementation of the WAL writing method for tar archive-based storage. It embeds the base WalWriteMethod structure as its first member, following PostgreSQL's pattern for method polymorphism. This structure manages the overall tar archive state, including the archive file descriptor, current file being written, and compression-related data when gzip compression is enabled. The structure coordinates the creation of tar-formatted archives containing WAL files during pg_basebackup streaming operations.

## Parameters / Member Variables
- `base`: The base WalWriteMethod structure containing common method operations, compression settings, synchronization flags, and error handling fields
- `tarfilename`: String pointer to the filename of the tar archive being created
- `fd`: File descriptor for the tar archive file
- `currentfile`: Pointer to the currently open TarMethodFile being written to within the tar archive
- `zp`: zlib compression stream pointer for gzip-compressed tar files (when HAVE_LIBZ is defined)
- `zlibOut`: Output buffer for zlib compression operations

## Dependencies
- Functions called/Symbols referenced:
  - [WalWriteMethod](../W/WalWriteMethod.md) (base structure)
  - [TarMethodFile](TarMethodFile.md) (file handle for individual files within the tar)
- Called from (representative examples):
  - [tar_write_compressed_data](../t/tar_write_compressed_data.md) (uses TarMethodData for compressed writes)
  - [tar_write](../t/tar_write.md) (writes data using TarMethodData)
  - [tar_open_for_write](../t/tar_open_for_write.md) (creates new files within TarMethodData archive)
  - tar_sync (synchronizes TarMethodData to disk)
  - tar_close (closes files within TarMethodData)
  - tar_finish (finalizes TarMethodData archive)
  - [tar_free](../t/tar_free.md) (deallocates TarMethodData)
  - [CreateWalTarMethod](../C/CreateWalTarMethod.md) (instantiates TarMethodData)

## Notes and Other Information
- This structure follows PostgreSQL's object-oriented programming pattern in C, where the base structure is embedded as the first member
- Supports gzip compression through conditional compilation when HAVE_LIBZ is defined
- The currentfile pointer tracks the active file being written within the tar archive
- Manages the overall tar archive state while individual TarMethodFile instances handle per-file data
- Used specifically in pg_basebackup's streaming backup functionality for tar-based WAL storage
- Part of the pluggable WAL writing method system alongside directory-based implementations
- Proper tar format compliance ensures compatibility with standard tar utilities
- Memory management for tarfilename and compression buffers must be handled throughout the structure's lifetime