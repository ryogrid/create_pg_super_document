# IsPartialXLogFileName

## Location
[src/include/access/xlog_internal.h:192-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L192-L199)

## Overview
IsPartialXLogFileName validates whether a given filename follows the PostgreSQL partial WAL segment file naming convention (standard WAL name with ".partial" suffix).

## Definition

```c
static inline bool
IsPartialXLogFileName(const char *fname)
```
## Detailed Description
IsPartialXLogFileName checks if a filename represents a partial WAL segment file by verifying three criteria: the total filename length matches the standard WAL filename length plus the ".partial" suffix length, the first 24 characters are valid hexadecimal digits, and the filename ends with ".partial". Partial WAL files are used by pg_receivewal and during archive recovery when a WAL segment might not be complete yet but needs to be archived or processed.

## Parameters / Member Variables
- `*fname`: The filename string to validate
## Dependencies
- Functions called/Symbols referenced:
  - XLOG_FNAME_LEN
  - strlen (standard C library)
  - strspn (standard C library)
  - strcmp (standard C library)
- Called from (representative examples):
  - [RemoveOldXlogFiles](../R/RemoveOldXlogFiles.md)
  - [CleanupPriorWALFiles](../C/CleanupPriorWALFiles.md)
  - [SetWALFileNameForCleanup](../S/SetWALFileNameForCleanup.md)
  - FundEndOfXLOG
  - [KillExistingXLOG](../K/KillExistingXLOG.md)

## Notes and Other Information
- This is an inline function defined in xlog_internal.h for performance
- Used specifically for identifying incomplete WAL segments that have the ".partial" suffix
- Critical during WAL streaming replication and archive recovery processes
- Ensures proper handling of partial WAL files that shouldn't be treated as complete segments
- The validation pattern is TTTTTTTTFFFFFFFFSSSSSSSS.partial where the first 24 chars are hexadecimal

## Simplified Source

```c
// Simplified version of IsPartialXLogFileName
static inline bool IsPartialXLogFileName(const char *fname) {
    // Check if filename has correct total length (24 chars + ".partial")
    if (strlen(fname) != XLOG_FNAME_LEN + strlen(".partial"))
        return false;

    // Check if first 24 characters are valid hexadecimal
    if (strspn(fname, "0123456789ABCDEF") != XLOG_FNAME_LEN)
        return false;

    // Check if filename ends with ".partial" suffix
    return strcmp(fname + XLOG_FNAME_LEN, ".partial") == 0;
}
```

Key simplifications made:
- Broke down the compound condition into clear, sequential checks
- Added early returns for better readability
- Emphasized the three validation criteria: length, hex digits, suffix
- Maintained the exact same logic while improving code clarity