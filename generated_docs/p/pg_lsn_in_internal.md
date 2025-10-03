# pg_lsn_in_internal

## Location
[src/backend/utils/adt/pg_lsn.c:29-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L29-L62)

## Overview
An internal function that parses a string representation of a PostgreSQL Log Sequence Number (LSN) and converts it to an XLogRecPtr value with error handling.

## Definition

```c
XLogRecPtr
pg_lsn_in_internal(const char *str, bool *have_error)
```
## Detailed Description
This function serves as the core parsing logic for LSN string input validation and conversion. It takes a string in the format "XXXXXXXX/XXXXXXXX" (where X represents hexadecimal digits) and converts it to a 64-bit XLogRecPtr value. The function performs thorough input validation including format checking, length validation, and proper error reporting through an output parameter rather than throwing exceptions.

The LSN format consists of two 32-bit hexadecimal numbers separated by a forward slash, representing the timeline ID and offset within that timeline. The function validates that each component contains only valid hexadecimal characters and doesn't exceed the maximum allowed length.

## Parameters / Member Variables
- `*str`: Input string containing the LSN in "XXXXXXXX/XXXXXXXX" format
- `*have_error`: Output parameter set to true if parsing fails, false on success
## Dependencies
- Functions called/Symbols referenced:
  - MAXPG_LSNCOMPONENT (constant defining maximum length of LSN components)
  - strspn (C library function for string span)
  - strtoul (C library function for string to unsigned long conversion)
  - InvalidXLogRecPtr (constant representing an invalid LSN)

- Called from (representative examples):
  - [check_recovery_target_lsn](../c/check_recovery_target_lsn.md)
  - [pg_lsn_in](pg_lsn_in.md)
  - PG_RETURN_LSN

## Notes and Other Information
- This is an internal function designed to provide robust error handling for LSN parsing
- Returns InvalidXLogRecPtr on error and sets *have_error to true
- The function performs strict validation of input format before attempting conversion
- Used as the foundation for user-facing LSN input functions that need different error handling strategies
- Each LSN component is limited by MAXPG_LSNCOMPONENT to prevent overflow conditions

## Simplified Source

```c
XLogRecPtr
pg_lsn_in_internal(const char *str, bool *have_error)
{
    int      len1, len2;
    uint32   id, off;
    XLogRecPtr result;

    *have_error = false;

    // Validate first component (before '/')
    len1 = strspn(str, "0123456789abcdefABCDEF");
    if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/')
    {
        *have_error = true;
        return InvalidXLogRecPtr;
    }

    // Validate second component (after '/')
    len2 = strspn(str + len1 + 1, "0123456789abcdefABCDEF");
    if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0')
    {
        *have_error = true;
        return InvalidXLogRecPtr;
    }

    // Parse hex components: timeline ID and offset
    id = (uint32) strtoul(str, NULL, 16);
    off = (uint32) strtoul(str + len1 + 1, NULL, 16);

    // Combine into 64-bit LSN
    result = ((uint64) id << 32) | off;

    return result;
}
```