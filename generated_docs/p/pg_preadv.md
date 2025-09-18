# pg_preadv

## Location
src/include/port/pg_iovec.h: 50 - 88

## Overview
A vectored read function that reads from multiple buffers in a single system call, with a reminder that on Windows this changes the current file position.

## Definition
```c
static inline ssize_t pg_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset)
```

## Detailed Description
pg_preadv provides a platform-independent wrapper around the preadv() system call for vectored I/O operations. It reads data from a file descriptor into multiple buffers specified by an iovec array at a given offset. The function has two implementation paths:

1. **Systems with preadv support**: Uses the native preadv() syscall, with an optimization for single iovec cases where it falls back to pread() to avoid argument copying overhead.

2. **Systems without preadv**: Implements vectored reading by iterating through the iovec array and calling pg_pread() for each buffer, manually tracking the offset and accumulating the total bytes read.

The function includes important platform-specific behavior: on Windows, unlike the POSIX preadv(), this operation changes the current file position as a side effect.

## Parameters
- `fd`: File descriptor to read from
- `iov`: Array of iovec structures specifying the buffers to read into
- `iovcnt`: Number of iovec structures in the array
- `offset`: File offset from which to start reading

## Dependencies
- Functions called/Symbols referenced:
  - [iovec](../i/iovec.md) (struct)
  - ssize_t (type)
  - pread (POSIX function)
  - preadv (POSIX function, if available)
  - pg_pread (PostgreSQL wrapper function)
- Called from:
  - FileReadV

## Notes and Other Information
- The function is marked as static inline for performance
- On systems with preadv support, single iovec operations are optimized to use pread() directly
- On Windows, this function has the side effect of changing the current file position, unlike POSIX preadv()
- Error handling: Returns -1 on error for the first buffer, or partial read count if error occurs on subsequent buffers
- The fallback implementation handles partial reads correctly by checking if fewer bytes were read than requested