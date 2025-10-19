# cloneFile

## Location
[src/bin/pg_upgrade/file.c:39-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/file.c#L39-L81)

## Overview
Creates a file clone/reflink from a source file to a destination file, providing efficient copy-on-write file duplication for relation files in PostgreSQL.

## Definition

```c
void
cloneFile(const char *src, const char *dst,
		  const char *schemaName, const char *relName)
```
## Detailed Description
The cloneFile function implements efficient file cloning using platform-specific system calls. It attempts to create a copy-on-write clone of a file, which shares disk blocks between source and destination until either file is modified. This is significantly more efficient than traditional file copying for large relation files during pg_upgrade operations.

The function provides two platform-specific implementations:
1. **macOS**: Uses the  system call with  flag
2. **Linux**: Uses the  ioctl operation on BTRFS and other filesystems that support reflinks

If cloning fails at any point, the function terminates the program with a fatal error message that includes the schema name, relation name, and file paths for debugging.

## Parameters / Member Variables
- `*src`: Source file path to clone from
- `*dst`: Destination file path to create as a clone
- `*schemaName`: SQL schema name of the relation (used only for error reporting)
- `*relName`: SQL relation name (used only for error reporting)
## Dependencies
- Functions called/Symbols referenced:
  - copyfile (macOS implementation)
  - open
  - ioctl (Linux implementation)
  - close
  - unlink
  - strerror
  - [pg_fatal](../p/pg_fatal.md)
  - PG_BINARY
  - pg_file_create_mode
- Called from (representative examples):
  - [transfer_relfile](../t/transfer_relfile.md)

## Notes and Other Information
- The function is conditionally compiled based on platform capabilities (,  for macOS,  and  for Linux)
- On Linux, the function opens the source file in read-only mode and creates the destination file with appropriate permissions
- If the clone operation fails on Linux, the partially created destination file is cleaned up using
- The function is primarily used during PostgreSQL upgrades to efficiently duplicate relation files
- Clone operations require filesystem support (e.g., BTRFS, APFS) and may fall back to regular copying in some upgrade scenarios

## Simplified Source

```c
void cloneFile(const char *src, const char *dst,
               const char *schemaName, const char *relName) {
#if defined(HAVE_COPYFILE) && defined(COPYFILE_CLONE_FORCE)
    // macOS implementation using copyfile()
    if (copyfile(src, dst, NULL, COPYFILE_CLONE_FORCE) < 0)
        pg_fatal("error while cloning relation \"%s.%s\" (\"%s\" to \"%s\"): %m",
                 schemaName, relName, src, dst);

#elif defined(__linux__) && defined(FICLONE)
    // Linux implementation using ioctl FICLONE
    int src_fd, dest_fd;

    // Open source file for reading
    if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
        pg_fatal("error while cloning relation \"%s.%s\": could not open file \"%s\": %m",
                 schemaName, relName, src);

    // Create destination file
    if ((dest_fd = open(dst, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
                        pg_file_create_mode)) < 0)
        pg_fatal("error while cloning relation \"%s.%s\": could not create file \"%s\": %m",
                 schemaName, relName, dst);

    // Perform the clone operation
    if (ioctl(dest_fd, FICLONE, src_fd) < 0) {
        int save_errno = errno;
        unlink(dst);  // Clean up on failure
        pg_fatal("error while cloning relation \"%s.%s\" (\"%s\" to \"%s\"): %s",
                 schemaName, relName, src, dst, strerror(save_errno));
    }

    close(src_fd);
    close(dest_fd);
#endif
}
```