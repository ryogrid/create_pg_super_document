# pvsnprintf

## Location
[src/common/psprintf.c:106-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/psprintf.c#L106-L151)

## Overview
A PostgreSQL utility function that attempts to format text data into a buffer using sprintf-style formatting, returning either the number of bytes written or an estimate of the required buffer size.

## Definition

```c
size_t
pvsnprintf(char *buf, size_t len, const char *fmt, va_list args)
```
## Detailed Description
The `pvsnprintf` function is a robust wrapper around the standard `vsnprintf` function that provides PostgreSQL-specific error handling and buffer size estimation. It serves as the core formatting engine used by `psprintf` and other PostgreSQL string formatting utilities.

Key characteristics:
- Wraps standard `vsnprintf` with PostgreSQL-specific error handling
- Returns actual bytes written on success (excluding null terminator)
- Returns estimated required buffer size on overflow (including space for null terminator)
- Provides different error reporting for backend vs frontend builds
- Includes overflow protection against `MaxAllocSize`
- Handles C99-compliant vsnprintf semantics with PostgreSQL adaptations

The function is designed to be used in retry loops where the caller can reallocate larger buffers based on the size estimate returned on overflow.

## Parameters / Member Variables
- `buf`: Destination buffer where formatted text will be written
- `len`: Size of the destination buffer in bytes
- `fmt`: A sprintf-style format string that controls formatting
- `args`: Variable argument list (va_list) containing arguments for the format string

## Dependencies
- Functions called/Symbols referenced:
  - `vsnprintf`: Standard C library function for formatted string writing
  - `elog`: PostgreSQL error logging function (backend builds)
  - `ereport`: PostgreSQL error reporting function (backend builds)
  - `fprintf`: Standard C library function (frontend builds)
  - `exit`: Standard C library function (frontend builds)
  - `MaxAllocSize`: PostgreSQL constant defining maximum allocation size
  - `EXIT_FAILURE`: Standard C library constant
  - `FRONTEND`: PostgreSQL build configuration macro

- Called from (representative examples):
  - [psprintf](psprintf.md): Dynamic string formatting function
  - `[appendStringInfoVA](../a/appendStringInfoVA.md)`: String buffer append function
  - [archprintf](../a/archprintf.md): pg_dump archive formatting function
  - [ahprintf](../a/ahprintf.md): pg_dump archive header formatting function
  - [tarPrintf](../t/tarPrintf.md): pg_dump tar format printing function

## Notes and Other Information
- **Return Value Semantics**: Not exactly C99-compliant; returns recommended buffer size rather than one less than needed
- **Error Handling**: Does not return error codes; instead exits via `elog(ERROR)` or `exit()` for fatal conditions
- **Buffer Size Estimation**: May require multiple iterations to get exact buffer size due to implementation variations
- **Memory Protection**: Includes overflow checks against `MaxAllocSize` to prevent excessive memory allocation
- **Frontend vs Backend**: Different error handling paths depending on build context
- **Thread Safety**: Callers must preserve errno when looping, especially for format strings containing '%m'
- **Usage Context**: Not recommended for use inside libpq due to its error handling behavior
- **Format String Support**: Supports all standard printf format specifiers plus PostgreSQL-specific extensions