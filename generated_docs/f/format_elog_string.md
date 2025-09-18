# format_elog_string

## Location
src/backend/utils/error/elog.c: 1654 - 1686

## Overview
Formats an error message string using printf-style formatting with PostgreSQL's error handling infrastructure and returns the formatted message.

## Definition
```c
char *format_elog_string(const char *fmt, ...)
```

## Detailed Description
The `format_elog_string` function is the core message formatting function in PostgreSQL's error logging system. It takes a printf-style format string and variable arguments, then creates a formatted error message using the previously saved error context from `pre_format_elog_string`.

The function creates a temporary ErrorData structure to hold the formatting context, switches to the ErrorContext memory context for allocation, and uses the EVALUATE_MESSAGE macro to perform the actual formatting. The function preserves the saved errno value and text domain that were stored by `pre_format_elog_string`, ensuring that the formatting process uses the correct error context even if errno has been modified during argument evaluation.

The formatted message is allocated in the ErrorContext and returned as a string. This function is typically used in conjunction with `pre_format_elog_string` to provide safe, context-preserving error message formatting.

## Parameters / Member Variables
- `fmt`: Printf-style format string for the error message
- `...`: Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - MemSet (macro/function)
  - PG_TEXTDOMAIN (macro)
  - EVALUATE_MESSAGE (macro)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (function)
  - ErrorContext (global variable)
- Called from (representative examples):
  - arch_module_check_errdetail
  - GUC_check_errmsg
  - GUC_check_errdetail
  - GUC_check_errhint

## Notes and Other Information
- Returns a formatted string allocated in ErrorContext
- Uses saved errno and domain values from `pre_format_elog_string`
- Supports printf-style formatting including PostgreSQL-specific %m (errno message)
- Part of the two-phase error formatting system (pre_format + format)
- The returned string should not be freed by the caller (managed by ErrorContext)
- Used extensively in GUC system and archive modules for error reporting
- Memory allocation is done in ErrorContext to ensure proper cleanup