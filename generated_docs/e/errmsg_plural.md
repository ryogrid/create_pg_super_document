# errmsg_plural

## Location
[src/backend/utils/error/elog.c:1180-1202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1180-L1202)

## Overview
Adds a primary error message text to the current error with support for pluralization, automatically selecting between singular and plural forms based on a count value.

## Definition
```c
int errmsg_plural(const char *fmt_singular, const char *fmt_plural, 
                  unsigned long n, ...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);
```

## Detailed Description
`errmsg_plural` provides internationalization-aware error message formatting with automatic plural form selection. The function chooses between the singular and plural format strings based on the value of parameter `n` using the current locale's plural rules.

This function is essential for creating grammatically correct error messages in multiple languages, as different languages have varying plural rules. The function integrates with PostgreSQL's internationalization framework to ensure proper message translation and plural form selection according to the active locale.

The function operates within PostgreSQL's error handling framework, managing memory context switching and recursion depth tracking for safe operation during error conditions.

## Parameters / Member Variables
- `fmt_singular`: Format string for the singular form of the error message (printf-style)
- `fmt_plural`: Format string for the plural form of the error message (printf-style) 
- `n`: Count value used to determine which plural form to use
- `...`: Variable arguments corresponding to format specifiers in both format strings

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (error data structure)
  - CHECK_STACK_DEPTH (recursion safety check)
  - EVALUATE_MESSAGE_PLURAL (plural message processing macro)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)

- Called from (representative examples):
  - [GetNewMultiXactId](../G/GetNewMultiXactId.md) (transaction ID management)
  - [SetMultiXactIdLimit](../S/SetMultiXactIdLimit.md) (multixact limit warnings)
  - [ReadTwoPhaseFile](../R/ReadTwoPhaseFile.md) (two-phase commit processing)
  - [perform_base_backup](../p/perform_base_backup.md) (backup operations)
  - [CopyFrom](../C/CopyFrom.md) (COPY command processing)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md) (function parsing with argument counts)

## Notes and Other Information
- Returns 0 (return value is not meaningful)
- Manages recursion depth to prevent infinite error loops
- Switches memory context during operation for safe memory management  
- Integrates with PostgreSQL's gettext-based internationalization system
- Both format strings should have the same format specifiers and argument positions
- The plural form selection follows locale-specific plural rules (not just n == 1 vs n != 1)
- Commonly used for reporting counts of database objects, rows affected, etc.