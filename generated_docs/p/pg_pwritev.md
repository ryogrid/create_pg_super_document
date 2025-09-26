# pg_pwritev

## Location
[src/include/port/pg_iovec.h:89-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_iovec.h#L89-L123)

## Overview
A vectored write function that writes from multiple buffers in a single system call, with a reminder that on Windows this changes the current file position.

## Definition
```c
static inline ssize_t pg_pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset)
```

## Detailed Description
pg_pwritev provides a platform-independent wrapper around the pwritev() system call for vectored I/O operations. It writes data from multiple buffers specified by an iovec array to a file descriptor at a given offset. The function has two implementation paths:

1. **Systems with pwritev support**: Uses the native pwritev() syscall, with an optimization for single iovec cases where it falls back to pwrite() to avoid argument copying overhead in the kernel.

2. **Systems without pwritev**: Implements vectored writing by iterating through the iovec array and calling pg_pwrite() for each buffer, manually tracking the offset and accumulating the total bytes written.

Like its read counterpart pg_preadv, this function includes important platform-specific behavior: on Windows, unlike the POSIX pwritev(), this operation changes the current file position as a side effect.

## Parameters
- `fd`: File descriptor to write to
- `iov`: Array of iovec structures specifying the buffers to write from
- `iovcnt`: Number of iovec structures in the array
- `offset`: File offset at which to start writing

## Dependencies
- Functions called/Symbols referenced:
  - [iovec](../i/iovec.md) (struct)
  - ssize_t (type)
  - pwrite (POSIX function)
  - pwritev (POSIX function, if available)
  - [pg_pwrite](pg_pwrite.md) (PostgreSQL wrapper function)
- Called from:
  - [FileWriteV](../F/FileWriteV.md)
  - [pg_pwritev_with_retry](pg_pwritev_with_retry.md)

## Notes and Other Information
- The function is marked as static inline for performance
- On systems with pwritev support, single iovec operations are optimized to use pwrite() directly
- On Windows, this function has the side effect of changing the current file position, unlike POSIX pwritev()
- Error handling: Returns -1 on error for the first buffer, or partial write count if error occurs on subsequent buffers
- The fallback implementation handles partial writes correctly by checking if fewer bytes were written than requested
- Used by higher-level PostgreSQL I/O functions that require vectored write operations