# copy_file_clone

## Location
[src/bin/pg_combinebackup/copy_file.c:213-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/copy_file.c#L213-L258)

## Overview
Clones or reflinks a file from source to destination using platform-specific optimization techniques, with optional checksum calculation.

## Definition

```c
static void
copy_file_clone(const char *src, const char *dest,
				pg_checksum_context *checksum_ctx)
```
## Detailed Description
The `copy_file_clone` function implements high-performance file copying using platform-specific cloning/reflink capabilities. On macOS, it uses the `copyfile` system call with `COPYFILE_CLONE_FORCE` flag. On Linux, it uses the `FICLONE` ioctl to create reflinks that share storage blocks until modified (copy-on-write). These techniques provide near-instantaneous copying for large files by creating metadata references rather than copying actual data blocks. If cloning fails, the function reports an error and cleans up partial files. After successful cloning, it separately calculates the checksum by reading the source file if needed.

## Parameters / Member Variables
- `src`: Path to the source file to clone from
- `dest`: Path to the destination file to create
- `checksum_ctx`: Pointer to checksum context for checksum calculation

## Dependencies  
- Functions called/Symbols referenced:
  - `copyfile` - macOS system call for file cloning (when available)
  - `open` - System call for file opening (Linux path)
  - `ioctl` - System call with FICLONE for Linux reflinks
  - `close` - System call for file closing
  - `unlink` - System call to remove failed destination file
  - `strerror` - Convert errno to error string
  - [checksum_file](checksum_file.md) - Calculate checksum of the cloned file
  - `pg_file_create_mode` - File creation permissions
- Called from:
  - [copy_file](copy_file.md) (src/bin/pg_combinebackup/copy_file.c:80) - as COPY_METHOD_CLONE strategy

## Notes and Other Information
- Platform-specific implementation with conditional compilation
- Provides dramatic performance improvements for large files through copy-on-write semantics
- Includes cleanup logic to remove partially created files on failure
- Checksum calculation is performed separately after cloning since data isn't read during clone
- Will fatal error on platforms that don't support file cloning
- Part of PostgreSQL's pg_combinebackup utility optimization strategies
- Location: src/bin/pg_combinebackup/copy_file.c:213-258

## Simplified Source

```c
static void
copy_file_clone(const char *src, const char *dest,
                pg_checksum_context *checksum_ctx)
{
#if defined(HAVE_COPYFILE) && defined(COPYFILE_CLONE_FORCE)
    // macOS: Use copyfile with cloning support
    if (copyfile(src, dest, NULL, COPYFILE_CLONE_FORCE) < 0)
        pg_fatal("error while cloning file \"%s\" to \"%s\": %m", src, dest);

#elif defined(__linux__) && defined(FICLONE)
    // Linux: Use FICLONE ioctl for reflink copy
    int src_fd, dest_fd;

    // Open source file
    if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
        pg_fatal("could not open file \"%s\": %m", src);

    // Create destination file
    if ((dest_fd = open(dest, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
                        pg_file_create_mode)) < 0)
        pg_fatal("could not create file \"%s\": %m", dest);

    // Perform reflink clone
    if (ioctl(dest_fd, FICLONE, src_fd) < 0)
    {
        int save_errno = errno;
        unlink(dest);  // Clean up failed destination
        pg_fatal("error while cloning file \"%s\" to \"%s\": %s",
                 src, dest, strerror(save_errno));
    }

    close(src_fd);
    close(dest_fd);

#else
    // Platform doesn't support file cloning
    pg_fatal("file cloning not supported on this platform");
#endif

    // Calculate checksum of the cloned file if needed
    checksum_file(src, checksum_ctx);
}
```