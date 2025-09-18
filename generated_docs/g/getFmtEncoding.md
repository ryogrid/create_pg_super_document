# getFmtEncoding

## Location
[src/fe_utils/string_utils.c:78-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L78-L100)

## Overview
A static helper function that returns the currently configured character encoding for identifier formatting functions, with fallback behavior for uninitialized states.

## Definition
```c
static int getFmtEncoding(void)
```

## Detailed Description
This function retrieves the current encoding setting used by fmtId() and fmtQualifiedId() functions for proper identifier quoting and escaping. It accesses the static fmtIdEncoding variable that is set by setFmtEncoding().

The function implements defensive programming practices with different behavior in debug versus production builds. If the encoding has not been explicitly set (fmtIdEncoding == -1), assertion builds will trigger an assertion failure to help developers identify missing setFmtEncoding() calls during development. In production builds, the function gracefully defaults to UTF-8 encoding to avoid crashes.

This dual behavior ensures robust error detection during development while maintaining stability in production environments where an unset encoding should not cause application failures.

## Parameters / Member Variables
- No parameters (void function)
- Returns: Integer encoding value (PostgreSQL encoding constant)

## Dependencies
- Functions called/Symbols referenced:
  - PG_UTF8 (constant for UTF-8 encoding)
  - fmtIdEncoding (static variable access)
  - Assert (macro for debug builds)
- Called from (representative examples):
  - [fmtId](../f/fmtId.md) (string_utils.c:250)
  - [fmtQualifiedId](../f/fmtQualifiedId.md) (string_utils.c:298)

## Notes and Other Information
- Returns the value set by setFmtEncoding(), or PG_UTF8 as fallback
- Assertion in debug builds helps identify programming errors where encoding was not properly initialized
- Production builds default to UTF-8 for safety, which is generally compatible with most systems
- The function is static (internal to string_utils.c) and not part of the public API
- Used internally by identifier formatting functions to ensure encoding-appropriate character escaping
- The fallback to UTF-8 provides reasonable behavior for most use cases since UTF-8 is widely supported