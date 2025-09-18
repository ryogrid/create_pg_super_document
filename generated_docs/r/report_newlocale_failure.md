# report_newlocale_failure

## Location
src/backend/utils/adt/pg_locale.c: 1525 - 1553

## Overview
A static utility function that provides standardized error reporting when newlocale() system calls fail, ensuring consistent error messages and errno handling across different operating systems.

## Definition


## Detailed Description
This function handles error reporting for failed newlocale() system calls in a platform-independent manner. It addresses inconsistent errno behavior across different operating systems - Windows doesn't provide useful error indication from _create_locale(), and BSD-derived platforms often don't set errno despite POSIX requirements. The function normalizes these behaviors by defaulting to ENOENT when errno is not set, and provides clear, user-friendly error messages that distinguish between "no such locale" and "no such file" scenarios.

## Parameters / Member Variables
- `localename`: The name of the locale that failed to be created, used in error messages to help users identify the problematic locale specification

## Dependencies
- Functions called/Symbols referenced:
  - errno (global variable)
  - ereport
  - errcode
  - errmsg
  - errdetail
  - ERRCODE_INVALID_PARAMETER_VALUE
  - ERROR
  - ENOENT
- Called from (representative examples):
  - pg_newlocale_from_collation (at lines 1645, 1656, 1660)

## Notes and Other Information
- This is a static function, only accessible within the pg_locale.c compilation unit
- Handles platform-specific differences in errno setting behavior for locale creation failures
- Preserves the original errno value before calling auxiliary functions that might modify it
- Provides detailed error context when the error is ENOENT, clarifying that it means "no such locale" rather than a file system error
- Essential for debugging locale-related issues across different operating systems