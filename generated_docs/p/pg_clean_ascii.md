# pg_clean_ascii

## Location
[src/common/string.c:86-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/string.c#L86-L132)

## Overview
A security-focused utility function that creates a sanitized copy of a string by replacing non-ASCII characters with hexadecimal escape sequences to prevent control characters from corrupting log files or terminals.

## Definition
```c
char *pg_clean_ascii(const char *str, int alloc_flags)
```

## Detailed Description
This function creates a newly allocated copy of the input string, replacing any non-ASCII characters (outside the range 32-126) with "\\xXX" escape sequences where XX is the hexadecimal representation of the byte. The function exists specifically to filter user-provided strings that could contain arbitrary control characters, preventing terminal corruption when administrators view log files.

The function allocates memory differently based on compile-time context: in frontend applications it uses , while in backend code it uses  with the provided allocation flags. The worst-case scenario requires 4 bytes per input character (for the escape sequence), plus a null terminator.

The filtering approach is conservative, allowing only printable ASCII characters (32-126) and escaping everything else, including extended ASCII and multi-byte Unicode characters.

## Parameters / Member Variables
- : The null-terminated input string to be sanitized
- : Memory allocation flags passed to  in backend code (ignored in frontend)

## Dependencies
- Functions called/Symbols referenced:
  - FRONTEND (compile-time macro)
  - malloc (standard C library function, frontend only)
  - palloc_extended (PostgreSQL memory allocator, backend only)
- Called from (representative examples):
  - check_application_name (src/backend/commands/variable.c:1074)
  - check_cluster_name (src/backend/commands/variable.c:1112)
  - ProcessStartupPacket (src/backend/tcop/backend_startup.c:772)

## Notes and Other Information
- The function documentation explicitly discourages its general use, preferring proper string handling over filtering
- Designed specifically for log file safety when dealing with untrusted user input like application names or cluster names
- Future improvements may include more intelligent Unicode handling rather than blanket non-ASCII replacement
- Uses  for safe hex formatting and includes assertion checks for buffer bounds
- Returns NULL if memory allocation fails
- The 4x size multiplier ensures adequate space even if every input character needs escaping