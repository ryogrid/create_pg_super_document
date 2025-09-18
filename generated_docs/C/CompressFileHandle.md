# CompressFileHandle

## Location
src/bin/pg_dump/compress_io.h: 98 - 100

## Overview
CompressFileHandle is a structure that provides a file-oriented interface for compressed I/O operations in pg_dump, encapsulating function pointers and data needed for opening, reading, writing, and managing compressed files across different compression algorithms.

## Definition
```c
typedef struct CompressFileHandle CompressFileHandle;
struct CompressFileHandle
{
    bool (*open_func)(const char *path, int fd, const char *mode, CompressFileHandle *CFH);
    bool (*open_write_func)(const char *path, const char *mode, CompressFileHandle *CFH);
    size_t (*read_func)(void *ptr, size_t size, CompressFileHandle *CFH);
    void (*write_func)(const void *ptr, size_t size, CompressFileHandle *CFH);
    char *(*gets_func)(char *s, int size, CompressFileHandle *CFH);
    int (*getc_func)(CompressFileHandle *CFH);
    bool (*eof_func)(CompressFileHandle *CFH);
    bool (*close_func)(CompressFileHandle *CFH);
    const char *(*get_error_func)(CompressFileHandle *CFH);
    pg_compress_specification compression_spec;
    void *private_data;
};
```

## Detailed Description
CompressFileHandle provides a unified file I/O interface for different compression algorithms used in pg_dump operations. It implements a strategy pattern similar to CompressorState but specifically focused on file-level operations rather than stream-level compression. The structure abstracts away the differences between compression libraries (gzip, LZ4, zstd, none) by providing common function pointers for file operations like open, read, write, and close.

## Parameters / Member Variables
- `open_func`: Function pointer to open a file using either path or file descriptor with specified mode
- `open_write_func`: Function pointer to open a file specifically for writing operations
- `read_func`: Function pointer to read up to specified size bytes from file into buffer
- `write_func`: Function pointer to write specified size bytes from buffer to file
- `gets_func`: Function pointer to read a line (up to size-1 chars) from file with null termination
- `getc_func`: Function pointer to read next character as unsigned char cast to int
- `eof_func`: Function pointer to test if end-of-file has been reached
- `close_func`: Function pointer to close the open file handle
- `get_error_func`: Function pointer to get error description string for operation failures
- `compression_spec`: Compression specification containing algorithm type and parameters
- `private_data`: Private data pointer for compression implementation-specific state

## Dependencies
- Functions called/Symbols referenced:
  - pg_compress_specification
  - InitCompressFileHandle
  - InitDiscoverCompressFileHandle
  - EndCompressFileHandle
- Called from (representative examples):
  - InitCompressFileHandleGzip (src/bin/pg_dump/compress_gzip.c:432)
  - InitCompressFileHandleLZ4 (src/bin/pg_dump/compress_lz4.c:804)
  - InitCompressFileHandleZstd (src/bin/pg_dump/compress_zstd.c:559)
  - InitCompressFileHandleNone (src/bin/pg_dump/compress_none.c:201)
  - RestoreArchive (src/bin/pg_dump/pg_backup_archiver.c:340)
  - SetOutput (src/bin/pg_dump/pg_backup_archiver.c:1678)

## Notes and Other Information
- Defined in src/bin/pg_dump/compress_io.h:98-193
- Used across multiple compression implementations and backup/restore modules
- Supports both path-based and file descriptor-based file opening
- Provides stdio-like interface (read, write, getc, gets, eof) for compressed files
- The structure enables transparent compression/decompression for file operations
- Mode strings follow standard C library conventions (r, rb, w, wb, a, ab)
- Error handling is centralized through get_error_func for consistent error reporting
- Part of the pg_dump file compression infrastructure for backup/restore operations