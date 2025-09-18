# appendShellStringNoError

## Location
[src/fe_utils/string_utils.c:594-697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L594-L697)

## Overview
Safely appends a string to a shell command buffer with proper platform-specific quoting, gracefully handling dangerous characters by omitting them and returning false.

## Definition
```c
bool appendShellStringNoError(PQExpBuffer buf, const char *str)
```

## Detailed Description
This function implements secure shell command construction with platform-specific quoting mechanisms. It handles the complex escaping requirements for both Unix-like systems and Windows, ensuring that strings are properly quoted to form exactly one shell argument. The function performs an optimization check first - if the string contains only safe characters (alphanumeric, dash, underscore, period, slash, colon), no quoting is applied.

On Unix systems, it uses single-quote wrapping with special handling for embedded single quotes. On Windows, it implements the complex two-layer escaping required by cmd.exe and the Windows command-line parsing system, using caret escaping and careful backslash handling around double quotes.

When LF or CR characters are encountered, they are silently omitted from the output and the function returns false to indicate the modification. This provides a non-fatal alternative to appendShellString().

## Parameters / Member Variables
- `buf`: Target PQExpBuffer where the shell-quoted string will be appended
- `str`: Input string to be quoted and appended (LF/CR characters will be omitted)

## Return Value
- Returns true if the string was processed without modifications (no LF/CR found)
- Returns false if LF or CR characters were encountered and omitted

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBufferChar (appends individual characters)
  - [appendPQExpBufferStr](appendPQExpBufferStr.md) (appends string segments)
  - strspn (checks for safe character optimization)
  - strlen (determines string length for optimization check)
- Called from (representative examples):
  - [appendShellString](appendShellString.md) (as the underlying implementation)
  - [psql_get_variable](../p/psql_get_variable.md) (in psql's common.c)

## Notes and Other Information
- Implements different quoting strategies based on the target platform (Unix vs Windows)
- The Windows implementation handles the complex two-layer interpretation: cmd.exe parsing and argv construction
- Safe character optimization avoids unnecessary quoting for simple strings containing only: a-z, A-Z, 0-9, -, _, ., /, :
- Unix implementation uses single quotes with '\'"'\"' sequence for embedded single quotes
- Windows implementation uses caret escaping with complex backslash doubling rules around double quotes
- Provides graceful error handling compared to appendShellString()'s fatal error approach
- Critical for security in PostgreSQL utilities that construct shell commands dynamically