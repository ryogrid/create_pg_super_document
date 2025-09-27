# pg_strftime

## Location
[src/timezone/strftime.c:128-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/strftime.c#L128-L150)

## Overview
PostgreSQL's version of the standard C library strftime function that converts a timestamp structure to a formatted string representation using pattern-based formatting.

## Definition

```c
size_t
pg_strftime(char *s, size_t maxsize, const char *format, const struct pg_tm *t)
```
## Detailed Description
pg_strftime is PostgreSQL's implementation of the strftime function for formatting timestamps. It takes a timestamp structure (pg_tm) and converts it to a string representation according to a specified format pattern. The function is designed to be similar to the standard C library strftime but uses PostgreSQL's own pg_tm structure and internal formatting logic through the _fmt function.

The function performs bounds checking to ensure the output doesn't exceed the buffer size and handles error conditions by setting appropriate errno values. It preserves the original errno value if no errors occur during processing.

## Parameters / Member Variables
- : Output buffer to store the formatted string
- : Maximum size of the output buffer (including null terminator)  
- : Format string containing conversion specifiers that define how the timestamp should be formatted
- : Pointer to a pg_tm structure containing the timestamp components to format

## Dependencies
- Functions called/Symbols referenced:
  - [_fmt](../f/_fmt.md) (core formatting function)
  - [pg_tm](pg_tm.md) (timestamp structure type)
  - IN_NONE (warning enumeration value)
  - EOVERFLOW (error code for buffer overflow)
  - ERANGE (error code for range error)
- Called from (representative examples):
  - [str_time](../s/str_time.md) (in xlog.c)
  - [build_backup_content](../b/build_backup_content.md) (in xlogbackup.c)  
  - [AddFileToBackupManifest](../A/AddFileToBackupManifest.md) (in backup_manifest.c)
  - [timeofday](../t/timeofday.md) (in timestamp.c)
  - [get_formatted_log_time](../g/get_formatted_log_time.md) (in elog.c)

## Notes and Other Information
- Returns 0 on error (buffer overflow or range error) and sets errno appropriately
- Returns the number of characters written (excluding null terminator) on success
- The function preserves the original errno value when successful
- Uses PostgreSQL's internal _fmt function for the actual formatting work
- Commonly used throughout PostgreSQL for log formatting, backup operations, and timestamp display

## Simplified Source

```c
// Simplified version of pg_strftime
size_t pg_strftime(char *s, size_t maxsize, const char *format, const struct pg_tm *t) {
    // Save current errno to restore later if no errors occur
    int saved_errno = errno;

    // Core logic: Format the timestamp using internal _fmt function
    char *result_end = _fmt(format, t, s, s + maxsize, &warn);

    // Error handling: Check if formatting failed
    if (!result_end) {
        errno = EOVERFLOW;
        return 0;
    }

    // Error handling: Check if buffer was exactly filled (no room for null terminator)
    if (result_end == s + maxsize) {
        errno = ERANGE;
        return 0;
    }

    // Success: Null-terminate the string and restore errno
    *result_end = '\0';
    errno = saved_errno;

    // Return number of characters written (excluding null terminator)
    return result_end - s;
}
```

Key simplifications made:
- Removed detailed variable declarations for clarity
- Added descriptive comments for each logical step
- Consolidated error handling into clear conditional blocks
- Focused on the main execution path: format, check errors, return result
- Abstracted the warning enum usage as it's not central to the main logic