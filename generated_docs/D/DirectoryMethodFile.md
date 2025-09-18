# DirectoryMethodFile

## Location
src/bin/pg_basebackup/walmethods.c: 82 - 96

## Overview
DirectoryMethodFile is a structure that represents an individual file handle for directory-based WAL writing in PostgreSQL's pg_basebackup utility, extending the base Walfile structure with directory-specific file management data.

## Definition
```c
typedef struct DirectoryMethodFile
{
    Walfile     base;
    int         fd;
    char       *fullpath;
    char       *temp_suffix;
#ifdef HAVE_LIBZ
    gzFile      gzfp;
#endif
#ifdef USE_LZ4
    LZ4F_compressionContext_t ctx;
    size_t      lz4bufsize;
    void       *lz4buf;
#endif
} DirectoryMethodFile;
```

## Detailed Description
DirectoryMethodFile extends the base Walfile structure to provide directory-specific file handling capabilities. It stores all the necessary data for managing an individual WAL file when using the directory storage method. The structure includes file descriptors for both regular and compressed files, path information, and compression-specific contexts and buffers. This allows the directory method to handle various compression formats (gzip, LZ4) while maintaining a consistent interface through the base Walfile structure.

## Parameters / Member Variables
- `base`: The base Walfile structure containing the write method reference, current position, and pathname
- `fd`: File descriptor for the regular (uncompressed) file
- `fullpath`: Complete filesystem path to the file being written
- `temp_suffix`: Temporary suffix used during file creation before final rename
- `gzfp`: gzip file pointer for compressed files (when HAVE_LIBZ is defined)
- `ctx`: LZ4 compression context for LZ4-compressed files (when USE_LZ4 is defined)
- `lz4bufsize`: Size of the LZ4 compression buffer
- `lz4buf`: Buffer used for LZ4 compression operations

## Dependencies
- Functions called/Symbols referenced:
  - Walfile (base structure)
- Called from (representative examples):
  - [dir_open_for_write](../d/dir_open_for_write.md) (creates and initializes DirectoryMethodFile instances)
  - [dir_write](../d/dir_write.md) (writes data using DirectoryMethodFile)
  - [dir_close](../d/dir_close.md) (closes and finalizes DirectoryMethodFile)
  - [dir_sync](../d/dir_sync.md) (synchronizes DirectoryMethodFile to disk)

## Notes and Other Information
- This structure follows PostgreSQL's object-oriented programming pattern in C, with the base Walfile embedded as the first member
- Supports multiple compression methods through conditional compilation (gzip via zlib, LZ4)
- The temp_suffix mechanism allows for atomic file operations by writing to temporary names before renaming
- File descriptor management handles both compressed and uncompressed file operations
- Used specifically for directory-based WAL storage in pg_basebackup streaming operations
- Memory management for fullpath, temp_suffix, and compression buffers must be handled carefully throughout the file's lifetime