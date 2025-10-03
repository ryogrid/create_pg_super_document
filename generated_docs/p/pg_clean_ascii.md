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
- `*str`: The null-terminated input string to be sanitized
- `alloc_flags`: Memory allocation flags passed to  in backend code (ignored in frontend)
## Dependencies
- Functions called/Symbols referenced:
  - FRONTEND (compile-time macro)
  - malloc (standard C library function, frontend only)
  - [palloc_extended](palloc_extended.md) (PostgreSQL memory allocator, backend only)
- Called from (representative examples):
  - [check_application_name](../c/check_application_name.md) (src/backend/commands/variable.c:1074)
  - [check_cluster_name](../c/check_cluster_name.md) (src/backend/commands/variable.c:1112)
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md) (src/backend/tcop/backend_startup.c:772)

## Notes and Other Information
- The function documentation explicitly discourages its general use, preferring proper string handling over filtering
- Designed specifically for log file safety when dealing with untrusted user input like application names or cluster names
- Future improvements may include more intelligent Unicode handling rather than blanket non-ASCII replacement
- Uses  for safe hex formatting and includes assertion checks for buffer bounds
- Returns NULL if memory allocation fails
- The 4x size multiplier ensures adequate space even if every input character needs escaping

## Simplified Source

```c
// Simplified version of pg_clean_ascii
char *pg_clean_ascii(const char *str, int alloc_flags) {
    // Calculate worst-case buffer size (each char could become \xXX = 4 bytes)
    size_t dstlen = strlen(str) * 4 + 1;

    // Allocate memory (frontend uses malloc, backend uses palloc_extended)
    char *dst;
#ifdef FRONTEND
    dst = malloc(dstlen);
#else
    dst = palloc_extended(dstlen, alloc_flags);
#endif

    if (!dst)
        return NULL;

    // Process each character in the input string
    size_t i = 0;
    for (const char *p = str; *p != '\0'; p++) {
        // Check if character is printable ASCII (32-126)
        if (*p < 32 || *p > 126) {
            // Non-ASCII: convert to \xXX escape sequence
            snprintf(&dst[i], dstlen - i, "\\x%02x", (unsigned char) *p);
            i += 4;
        } else {
            // ASCII: copy character as-is
            dst[i] = *p;
            i++;
        }
    }

    // Null-terminate the result
    dst[i] = '\0';
    return dst;
}
```

Key simplifications made:
- Removed assertion checks for clarity
- Consolidated variable declarations
- Added clear comments explaining the logic flow
- Focused on the core algorithm: scan, test, escape or copy
- Maintained the essential security filtering behavior