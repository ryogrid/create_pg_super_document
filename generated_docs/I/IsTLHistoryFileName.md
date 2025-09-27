# IsTLHistoryFileName

## Location
[src/include/access/xlog_internal.h:224-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L224-L231)

## Overview
IsTLHistoryFileName is an inline function that determines whether a given filename conforms to PostgreSQL's timeline history file naming convention by validating the format and structure.

## Definition

```c
static inline bool
IsTLHistoryFileName(const char *fname)
```
## Detailed Description
This function validates whether a filename matches the expected format for timeline history files in PostgreSQL. It performs three specific checks to ensure the filename follows the exact pattern "TTTTTTTT.history" where:
1. The total length equals exactly 8 characters plus the length of ".history"
2. The first 8 characters are all hexadecimal digits (0-9, A-F)
3. The remaining part is exactly ".history"

This validation is essential for file processing operations that need to distinguish timeline history files from other files in the WAL directory structure.

## Parameters / Member Variables
- : The filename string to validate against timeline history file naming convention

## Dependencies
- Functions called/Symbols referenced:
  - strlen (C standard library function for string length)
  - strspn (C standard library function for character span validation)
  - strcmp (C standard library function for string comparison)
- Called from (representative examples):
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md) (archives WAL files and timeline history files)
  - [perform_base_backup](../p/perform_base_backup.md) (handles timeline history during base backup)
  - [ready_file_comparator](../r/ready_file_comparator.md) (sorts files for archival processing)

## Notes and Other Information
- This is an inline function defined in the header for performance optimization since it's used in file processing loops
- The function is case-sensitive and expects uppercase hexadecimal digits (A-F, not a-f)
- Timeline history files are critical metadata files that track timeline branching during recovery
- The validation helps ensure only properly formatted timeline history files are processed
- Used primarily in archival and backup operations where distinguishing file types is essential
- The function provides a reliable way to identify timeline history files without filesystem operations

## Simplified Source

```c
// Simplified version of IsTLHistoryFileName
static inline bool IsTLHistoryFileName(const char *fname) {
    // Check if filename has correct total length (8 hex chars + ".history")
    if (strlen(fname) != 8 + strlen(".history")) {
        return false;
    }

    // Check if first 8 characters are all hexadecimal digits
    if (strspn(fname, "0123456789ABCDEF") != 8) {
        return false;
    }

    // Check if the extension is exactly ".history"
    return strcmp(fname + 8, ".history") == 0;
}
```

Key simplifications made:
- Broke down the single complex boolean expression into three clear validation steps
- Added descriptive comments for each validation check
- Used early returns for better readability and flow
- Maintained the exact same logic and functionality as the original