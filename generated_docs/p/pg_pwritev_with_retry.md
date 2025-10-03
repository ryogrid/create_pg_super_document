# pg_pwritev_with_retry

## Location
[src/common/file_utils.c:637-686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L637-L686)

## Overview
A convenience wrapper around pg_pwritev() that handles partial writes by automatically retrying until all data is written or an error occurs.

## Definition

```c
ssize_t
pg_pwritev_with_retry(int fd, const struct iovec *iov, int iovcnt, off_t offset)
```
## Detailed Description
pg_pwritev_with_retry provides a robust interface for vectored positional writes that handles the common case where the underlying system call (pg_pwritev) may only write part of the requested data. The function automatically retries the write operation, adjusting the iovec array and offset as needed, until either all data is successfully written or an error occurs.

The function creates a local copy of the iovec array to allow modification during retry attempts without affecting the caller's original array. It uses compute_remaining_iovec() to calculate what data remains to be written after each partial write.

Key behaviors:
- Automatically handles partial writes by retrying until completion
- Maintains file offset tracking across retry attempts  
- Uses a stack-allocated iovec copy limited by PG_IOV_MAX for safety
- Returns total bytes written on success, -1 on error
- On error, the amount of data actually written is unspecified

## Parameters / Member Variables
- `fd`: File descriptor to write to
- `*iov`: Array of iovec structures describing the data buffers to write
- `iovcnt`: Number of iovec structures in the array (must not exceed PG_IOV_MAX)
- `offset`: File offset at which to begin writing
## Dependencies
- Functions called/Symbols referenced:
  - [pg_pwritev](pg_pwritev.md): Underlying vectored positional write function
  - [compute_remaining_iovec](../c/compute_remaining_iovec.md): Helper to calculate remaining data after partial writes
  - PG_IOV_MAX: Maximum number of iovec entries that can be safely handled
  - [iovec](../i/iovec.md): Standard vectored I/O structure
- Called from (representative examples):
  - [pg_pwrite_zeros](pg_pwrite_zeros.md): Uses this function for writing zero-filled blocks

## Notes and Other Information
- The function validates that iovcnt does not exceed PG_IOV_MAX to ensure safe stack allocation
- Includes conditional compilation support for SIMULATE_SHORT_WRITE testing 
- The iovec array parameter becomes read-only after the first iteration since modifications are made to the local copy
- Error handling follows POSIX conventions: returns -1 on error with errno set appropriately
- This function is part of PostgreSQL's cross-platform file I/O abstraction layer

## Simplified Source

```c
ssize_t pg_pwritev_with_retry(int fd, const struct iovec *iov, int iovcnt, off_t offset)
{
    struct iovec iov_copy[PG_IOV_MAX];
    ssize_t sum = 0;
    ssize_t part;

    // Validate iovec count doesn't exceed our buffer
    if (iovcnt > PG_IOV_MAX) {
        errno = EINVAL;
        return -1;
    }

    do {
        // Write as much as possible
        part = pg_pwritev(fd, iov, iovcnt, offset);
        if (part < 0)
            return -1;

        // Track progress
        sum += part;
        offset += part;

        // Calculate what's left to write, using our modifiable copy
        iovcnt = compute_remaining_iovec(iov_copy, iov, iovcnt, part);
        iov = iov_copy;
    } while (iovcnt > 0);

    return sum;  // Total bytes written
}
```