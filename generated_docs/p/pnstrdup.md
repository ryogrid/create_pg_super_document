# pnstrdup

## Location
[src/backend/utils/mmgr/mcxt.c:1706-1722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1706-L1722)

## Overview
A utility function that creates a null-terminated string copy from an input string that may not be null-terminated, with length specified explicitly.

## Definition

```c
char *
pnstrdup(const char *in, Size len)
```
## Detailed Description
pnstrdup extends the functionality of pstrdup by handling strings that are not necessarily null-terminated. It creates a new null-terminated string copy from an input buffer, copying at most 'len' characters or until a null terminator is encountered (whichever comes first). The function ensures the result is always null-terminated by appending a null byte.

The function uses strnlen() to determine the actual length to copy (which may be less than 'len' if a null terminator is found earlier), allocates memory using palloc(), copies the data with memcpy(), and ensures null termination. This is particularly useful when working with string data from external sources or when extracting substrings.

## Parameters / Member Variables
- `in`: The input string buffer to copy from (may not be null-terminated)
- `len`: The maximum number of characters to copy from the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - strnlen
  - [palloc](palloc.md)
  - memcpy
- Called from (representative examples):
  - [makeMultirangeTypeName](../m/makeMultirangeTypeName.md)
  - [llvm_split_symbol_name](../l/llvm_split_symbol_name.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [pgstat_clip_activity](pgstat_clip_activity.md)
  - [jsonb_put_escaped_value](../j/jsonb_put_escaped_value.md)
  - [pchomp](pchomp.md)

## Notes and Other Information
- Unlike pstrdup, this function can safely handle non-null-terminated input strings
- The function automatically determines the effective length using strnlen(), which stops at the first null character or at 'len' characters
- Memory is allocated in the current memory context via palloc()
- Widely used throughout PostgreSQL for handling string data from various sources including JSON processing, text search, formatting functions, and replication
- Located in src/backend/utils/mmgr/mcxt.c at lines 1706-1722
- Essential for safe string handling when dealing with potentially unterminated character arrays