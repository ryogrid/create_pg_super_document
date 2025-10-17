# tarPrintf

## Location
[src/bin/pg_dump/pg_backup_tar.c:954-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L954-L987)

## Overview
A static function that provides formatted string output functionality for TAR archive members, similar to printf but writes to a TAR member stream.

## Definition
```c
static int tarPrintf(TAR_MEMBER *th, const char *fmt, ...)
```

## Detailed Description
This function implements a printf-like interface for writing formatted text data to TAR archive members. It dynamically allocates a buffer to hold the formatted string, starting with an initial size assumption of 128 bytes and growing as needed. The function uses a retry loop to handle cases where the initial buffer is too small, reallocating with the exact required size. After successful formatting, it writes the data to the TAR member using tarWrite and returns the number of bytes written.

## Parameters / Member Variables
- `th`: Pointer to TAR_MEMBER structure representing the target TAR archive member
- `fmt`: Format string following printf conventions
- `...`: Variable arguments corresponding to the format string placeholders

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - [pvsnprintf](../p/pvsnprintf.md) (PostgreSQL variant of vsnprintf for safe formatting)
  - [tarWrite](tarWrite.md) (writes data to TAR member)
  - [TAR_MEMBER](../T/TAR_MEMBER.md) (struct type)
- Called from (representative examples):
  - [_CloseArchive](../C/_CloseArchive.md) (for writing TAR metadata)
  - [_StartLO](../S/_StartLO.md) (for Large Object operations)

## Notes and Other Information
- Uses dynamic memory allocation with automatic resizing to handle strings of any length
- Preserves errno state across the operation to avoid interfering with error reporting
- Returns the actual number of bytes written to the TAR member
- Part of the TAR archive support functionality in pg_dump
- Located in src/bin/pg_dump/pg_backup_tar.c:954-987

## Simplified Source

```c
static int tarPrintf(TAR_MEMBER *th, const char *fmt, ...) {
    int save_errno = errno;
    char *p;
    size_t len = 128;  // Initial buffer size
    size_t cnt;

    // Loop until buffer is large enough
    for (;;) {
        va_list args;

        // Allocate buffer
        p = (char *) pg_malloc(len);

        // Try to format the string
        errno = save_errno;
        va_start(args, fmt);
        cnt = pvsnprintf(p, len, fmt, args);
        va_end(args);

        // Success if formatted string fits in buffer
        if (cnt < len)
            break;

        // Buffer too small, try again with exact size needed
        free(p);
        len = cnt;
    }

    // Write formatted string to TAR member
    cnt = tarWrite(p, cnt, th);
    free(p);
    return (int) cnt;
}
```