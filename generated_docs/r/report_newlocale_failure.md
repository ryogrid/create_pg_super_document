# report_newlocale_failure

## Location
[src/backend/utils/adt/pg_locale.c:1525-1553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1525-L1553)

## Overview
A static utility function that provides standardized error reporting when newlocale() system calls fail, ensuring consistent error messages and errno handling across different operating systems.

## Definition

```c
static void
report_newlocale_failure(const char *localename)
```
## Detailed Description
This function handles error reporting for failed newlocale() system calls in a platform-independent manner. It addresses inconsistent errno behavior across different operating systems - Windows doesn't provide useful error indication from _create_locale(), and BSD-derived platforms often don't set errno despite POSIX requirements. The function normalizes these behaviors by defaulting to ENOENT when errno is not set, and provides clear, user-friendly error messages that distinguish between "no such locale" and "no such file" scenarios.

## Parameters / Member Variables
- `localename`: The name of the locale that failed to be created, used in error messages to help users identify the problematic locale specification

## Dependencies
- Functions called/Symbols referenced:
  - errno (global variable)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - ERRCODE_INVALID_PARAMETER_VALUE
  - ERROR
  - ENOENT
- Called from (representative examples):
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md) (at lines 1645, 1656, 1660)

## Notes and Other Information
- This is a static function, only accessible within the pg_locale.c compilation unit
- Handles platform-specific differences in errno setting behavior for locale creation failures
- Preserves the original errno value before calling auxiliary functions that might modify it
- Provides detailed error context when the error is ENOENT, clarifying that it means "no such locale" rather than a file system error
- Essential for debugging locale-related issues across different operating systems

## Simplified Source
```c
static void
report_newlocale_failure(const char *localename)
{
    int save_errno;

    // Default to ENOENT if errno not set (Windows/BSD compatibility)
    if (errno == 0)
        errno = ENOENT;

    // Save errno before calling error functions
    save_errno = errno;

    // Report error with appropriate detail message
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("could not create locale \"%s\": %m", localename),
             (save_errno == ENOENT ?
              errdetail("The operating system could not find any locale data for the locale name \"%s\".",
                        localename) : 0)));
}
```