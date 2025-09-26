# ascii_safe_strlcpy

## Location
[src/backend/utils/adt/ascii.c:174-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ascii.c#L174-L199)

## Overview
A safe string copy function that converts arbitrary backend-safe encoded strings to ASCII by replacing non-ASCII bytes with question marks.

## Definition
```c
void ascii_safe_strlcpy(char *dest, const char *src, size_t destsiz)
```

## Detailed Description
This function provides a safe way to copy strings in PostgreSQL contexts where error reporting is not available (particularly in the postmaster process). It behaves similarly to the standard strlcpy() function but with a key difference: it ensures the output is always valid ASCII by replacing any non-ASCII characters with question marks (?).

The function preserves printable ASCII characters (32-127), common whitespace characters (newline, carriage return, tab), and null terminators. All other bytes are replaced with ?. This makes it safe to use with arbitrary input encodings without risking invalid character sequences in the output.

The function is specifically designed to not trigger ereport(ERROR) since it may be called from the postmaster where error handling is limited.

## Parameters / Member Variables
- `dest`: Destination buffer to receive the ASCII-safe copy
- `src`: Source string to copy from (in arbitrary backend-safe encoding)
- `destsiz`: Size of the destination buffer including space for null terminator

## Dependencies
- Functions called/Symbols referenced:
  - None (self-contained implementation)
- Called from:
  - [BackgroundWorkerStateChange](../B/BackgroundWorkerStateChange.md) (in bgworker.c, multiple calls)
  - [pgstat_get_crashed_backend_activity](../p/pgstat_get_crashed_backend_activity.md) (in backend_status.c)

## Notes and Other Information
- Must not trigger ereport(ERROR) as it is called in postmaster context
- Handles corner case of zero-sized destination buffer safely
- Preserves printable ASCII (32-127) and common whitespace characters
- Replaces all other bytes with ? character for safety
- Does not return a value (unlike standard strlcpy)
- Uses unsigned char internally to avoid compiler warnings
- Always null-terminates the destination string if destsiz > 0