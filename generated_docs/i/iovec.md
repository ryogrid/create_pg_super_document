# iovec

## Location
[src/include/port/pg_iovec.h:25-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_iovec.h#L25-L38)

## Overview
A POSIX-compatible structure used for vectored I/O operations, defining a buffer with its base address and length.

## Definition
```c
struct iovec
{
    void       *iov_base;
    size_t      iov_len;
};
```

## Detailed Description
The iovec structure is a fundamental component of vectored I/O operations, representing a single buffer in an array of buffers that can be read from or written to in a single system call. In PostgreSQL, this structure is conditionally defined for Windows systems, as POSIX-compliant systems already provide it through `<sys/uio.h>`.

The structure enables scatter-gather I/O operations, where data can be read into or written from multiple non-contiguous memory buffers in a single system call, improving efficiency compared to multiple separate I/O operations.

PostgreSQL defines its own version of the structure on Windows (where it is not natively available) to maintain cross-platform compatibility for vectored I/O operations. This allows the same code to work on both POSIX-compliant systems and Windows.

## Member Variables
- `iov_base`: Pointer to the start of the buffer (can be any data type via void pointer)
- `iov_len`: Size of the buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - IOV_MAX (constant defined in the same header)
- Used by (representative examples):
  - [pg_preadv](../p/pg_preadv.md)
  - [pg_pwritev](../p/pg_pwritev.md)
  - FileReadV
  - FileWriteV
  - [mdreadv](../m/mdreadv.md)
  - [mdwritev](../m/mdwritev.md)
  - [buffers_to_iovec](../b/buffers_to_iovec.md)
  - pg_pwritev_with_retry

## Notes and Other Information
- Only defined on Windows systems; POSIX systems use the system-provided definition from `<sys/uio.h>`
- Part of PostgreSQL's cross-platform abstraction layer for vectored I/O
- Used extensively throughout PostgreSQL's storage layer for efficient bulk I/O operations
- The structure layout is compatible with the POSIX standard to ensure portability
- Associated with IOV_MAX and PG_IOV_MAX constants that define limits on the number of iovec structures that can be used in a single operation
- Enables scatter-gather I/O patterns that are crucial for performance in database storage systems