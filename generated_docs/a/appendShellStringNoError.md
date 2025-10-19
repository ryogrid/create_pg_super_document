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
  - [appendPQExpBufferChar](appendPQExpBufferChar.md) (appends individual characters)
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

## Simplified Source

```c
bool appendShellStringNoError(PQExpBuffer buf, const char *str) {
    bool ok = true;
    const char *p;

    // Optimization: no quoting needed for safe characters only
    if (*str != '\0' &&
        strspn(str, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./:")
        == strlen(str)) {
        appendPQExpBufferStr(buf, str);
        return ok;
    }

#ifndef WIN32
    // Unix: single-quote wrapping with special quote handling
    appendPQExpBufferChar(buf, '\'');
    for (p = str; *p; p++) {
        if (*p == '\n' || *p == '\r') {
            ok = false;  // Skip dangerous characters
            continue;
        }

        if (*p == '\'')
            appendPQExpBufferStr(buf, "'\"'\"'");  // Escape embedded quotes
        else
            appendPQExpBufferChar(buf, *p);
    }
    appendPQExpBufferChar(buf, '\'');

#else  // WIN32
    // Windows: complex two-layer escaping for cmd.exe and argv parsing
    int backslash_run_length = 0;

    appendPQExpBufferStr(buf, "^\"");  // Opening escaped quote
    for (p = str; *p; p++) {
        if (*p == '\n' || *p == '\r') {
            ok = false;  // Skip dangerous characters
            continue;
        }

        // Handle backslash-quote sequences specially
        if (*p == '"') {
            // Double backslashes before quotes
            while (backslash_run_length) {
                appendPQExpBufferStr(buf, "^\\");
                backslash_run_length--;
            }
            appendPQExpBufferStr(buf, "^\\");
        } else if (*p == '\\') {
            backslash_run_length++;
        } else {
            backslash_run_length = 0;
        }

        // Caret-escape special characters (except alphanumeric)
        if (!((*p >= 'a' && *p <= 'z') ||
              (*p >= 'A' && *p <= 'Z') ||
              (*p >= '0' && *p <= '9')))
            appendPQExpBufferChar(buf, '^');
        appendPQExpBufferChar(buf, *p);
    }

    // Handle trailing backslashes before closing quote
    while (backslash_run_length) {
        appendPQExpBufferStr(buf, "^\\");
        backslash_run_length--;
    }
    appendPQExpBufferStr(buf, "^\"");  // Closing escaped quote
#endif

    return ok;
}
```