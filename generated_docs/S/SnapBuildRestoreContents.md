# SnapBuildRestoreContents

## Location
[src/backend/replication/logical/snapbuild.c:2081-2117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L2081-L2117)

## Overview
SnapBuildRestoreContents is a low-level utility function that reads a specified amount of data from a file descriptor into a destination buffer with proper error handling and wait event reporting.

## Definition
```c
static void SnapBuildRestoreContents(int fd, char *dest, Size size, const char *path)
```

## Detailed Description
This helper function performs a single, complete read operation from a file descriptor, ensuring that exactly the requested amount of data is read. It provides:

1. **Wait Event Reporting**: Reports WAIT_EVENT_SNAPBUILD_READ to the statistics collector during the read operation
2. **Complete Read Guarantee**: Ensures that exactly the requested number of bytes are read, not fewer
3. **Error Handling**: Distinguishes between I/O errors and incomplete reads (data corruption)
4. **Resource Cleanup**: Closes the file descriptor on error to prevent resource leaks

The function is specifically designed for reading serialized snapshot components where partial reads indicate data corruption or file truncation.

## Parameters / Member Variables
- `fd`: File descriptor to read from (must be open for reading)
- `dest`: Destination buffer to store the read data
- `size`: Number of bytes to read from the file
- `path`: File path string used only for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end
  - read (system call)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - ereport/errcode_for_file_access
  - ERRCODE_DATA_CORRUPTED (error code constant)
- Called from (representative examples):
  - [SnapBuildRestore](SnapBuildRestore.md) (multiple times for different data components)

## Notes and Other Information
- Used exclusively by SnapBuildRestore to read different portions of the serialized snapshot file
- Treats partial reads as data corruption rather than normal end-of-file conditions
- The path parameter is only used for generating meaningful error messages
- Reports different error codes: file access errors for I/O problems, data corruption errors for incomplete reads
- Wait event reporting allows monitoring of snapshot restoration I/O operations
- Performs cleanup by closing the file descriptor before throwing errors, preventing file descriptor leaks
- Assumes that the caller has already positioned the file descriptor correctly for the next read operation

## Simplified Source

```c
static void
SnapBuildRestoreContents(int fd, char *dest, Size size, const char *path)
{
    int readBytes;

    // Report wait event and perform the read operation
    pgstat_report_wait_start(WAIT_EVENT_SNAPBUILD_READ);
    readBytes = read(fd, dest, size);
    pgstat_report_wait_end();

    // Ensure we read exactly the expected amount
    if (readBytes != size)
    {
        int save_errno = errno;
        CloseTransientFile(fd);

        // Report appropriate error based on failure type
        if (readBytes < 0)
        {
            errno = save_errno;
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not read file \"%s\": %m", path)));
        }
        else
        {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                           errmsg("could not read file \"%s\": read %d of %zu",
                                  path, readBytes, size)));
        }
    }
}
```