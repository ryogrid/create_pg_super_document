# strlcpy

## Location
[src/port/strlcpy.c:45-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/strlcpy.c#L45-L71)

## Overview
A safe string copying function that provides bounds-checking and guaranteed null termination, serving as a secure alternative to strcpy and strncpy in PostgreSQL's portability layer.

## Definition

```c
size_t
strlcpy(char *dst, const char *src, size_t siz)
```
## Detailed Description
The strlcpy function copies strings with a focus on preventing buffer overflows and ensuring proper null termination. Unlike strcpy (which is unsafe) and strncpy (which doesn't guarantee null termination), strlcpy provides a reliable interface for string copying operations.

The function copies at most siz-1 characters from the source string to the destination buffer, always null-terminating the result (unless siz is 0). It returns the length of the source string, allowing the caller to detect truncation by comparing the return value with the buffer size.

This implementation is part of PostgreSQL's portability layer (src/port/), providing the function on systems where it's not natively available. The function follows the OpenBSD strlcpy specification and is widely used throughout the PostgreSQL codebase for safe string operations.

## Parameters / Member Variables
- : Destination buffer where the string will be copied
- : Source string to be copied (null-terminated)
- : Size of the destination buffer, including space for null terminator

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (implements basic string copying using pointer arithmetic)
- Called from (representative examples):
  - ParseCommitRecord (src/backend/access/rmgrdesc/xactdesc.c:119)
  - SimpleLruInit (src/backend/access/transam/slru.c:347)
  - XLogRestorePoint (src/backend/access/transam/xlog.c:8100)
  - DefineRelation (src/backend/commands/tablecmds.c:725)
  - WalReceiverMain (src/backend/replication/walreceiver.c:251)
  - process_postgres_switches (src/backend/tcop/postgres.c:4031)
  - hash_create (src/backend/utils/hash/dynahash.c:467)
  - InitPostgres (src/backend/utils/init/postinit.c:1106)
  - PQcancel (src/interfaces/libpq/fe-cancel.c:477)
  - join_path_components (src/port/path.c:289)

## Notes and Other Information
- **Return Value**: Returns the length of the source string (strlen(src)). If the return value is >= siz, truncation occurred.
- **Safety Features**: Always null-terminates the destination (unless siz == 0), preventing buffer overruns and ensuring valid C strings.
- **Performance**: Efficient single-pass implementation that copies characters while counting source length.
- **Portability**: Part of PostgreSQL's portability layer, compiled only on systems lacking native strlcpy support.
- **Usage Pattern**: Commonly used throughout PostgreSQL for safe string copying in configuration parsing, path handling, error message formatting, and data structure initialization.
- **Historical Context**: Based on the OpenBSD strlcpy function, created to address security issues with strcpy and usability issues with strncpy.
- **Error Handling**: The function itself cannot fail, but callers can detect truncation by checking if the return value >= siz.