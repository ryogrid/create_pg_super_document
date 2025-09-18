# tarPrintf

## Location
src/bin/pg_dump/pg_backup_tar.c: 954 - 987

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
  - pg_malloc (memory allocation)
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