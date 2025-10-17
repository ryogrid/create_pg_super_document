# Zstd_open

## Location
[src/bin/pg_dump/compress_zstd.c:504-541](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L504-L541)

## Overview
Opens a file for Zstd compression or decompression, initializing the necessary state structures.

## Definition
```c
static bool Zstd_open(const char *path, int fd, const char *mode, CompressFileHandle *CFH)
```

## Detailed Description
This function initializes a Zstd-compressed file handle by opening the underlying file and allocating the necessary state structure. It supports opening files either by path or by file descriptor, making it flexible for different use cases in PostgreSQL's dump utility.

The function performs the following key operations:
1. Allocates memory for the ZstdCompressorState structure using PostgreSQL's memory management
2. Opens the file using either fdopen() (for file descriptor) or fopen() (for file path)
3. Initializes the state structure with the opened file pointer
4. Handles memory cleanup on failure

The function uses PostgreSQL's extended malloc with specific flags to avoid throwing errors on out-of-memory conditions and to zero-initialize the allocated memory.

## Parameters / Member Variables
- `path`: File path to open (used when fd < 0)
- `fd`: File descriptor to use (when >= 0, takes precedence over path)
- `mode`: File open mode (e.g., "rb", "wb")
- `CFH`: Compressed file handle to initialize

## Dependencies
- Functions called/Symbols referenced:
  - [ZstdCompressorState](ZstdCompressorState.md)
  - [CompressFileHandle](../C/CompressFileHandle.md)
  - [pg_malloc_extended](../p/pg_malloc_extended.md)
  - MCXT_ALLOC_NO_OOM
  - MCXT_ALLOC_ZERO
  - fdopen
  - dup
  - fopen
  - [pg_free](../p/pg_free.md)
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (as part of function pointer assignment)

## Notes and Other Information
- This is a static function within the Zstd compression module
- Returns true on successful open, false on failure
- Handles both file path and file descriptor opening methods
- Uses PostgreSQL's memory management functions for proper integration
- Clears CFH->private_data initially to avoid pointing to invalid memory on error
- The function duplicates file descriptors using dup() to ensure proper ownership
- Memory allocation uses MCXT_ALLOC_NO_OOM to handle out-of-memory gracefully
- Part of the compression abstraction layer providing uniform file opening interface
- Sets errno appropriately on memory allocation failure

## Simplified Source

```c
static bool
Zstd_open(const char *path, int fd, const char *mode,
          CompressFileHandle *CFH)
{
    FILE *fp;
    ZstdCompressorState *zstdcs;

    // Clear state to avoid dangling pointers on error
    CFH->private_data = NULL;

    // Allocate Zstd state with no-OOM handling
    zstdcs = (ZstdCompressorState *) pg_malloc_extended(sizeof(*zstdcs),
                                    MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
    if (!zstdcs) {
        errno = ENOMEM;
        return false;
    }

    // Open file by descriptor or path
    if (fd >= 0)
        fp = fdopen(dup(fd), mode);
    else
        fp = fopen(path, mode);

    if (fp == NULL) {
        pg_free(zstdcs);
        return false;
    }

    // Initialize state and return success
    zstdcs->fp = fp;
    CFH->private_data = zstdcs;

    return true;
}
```