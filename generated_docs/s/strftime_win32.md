# strftime_win32

## Location
[src/backend/utils/adt/pg_locale.c:758-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L758-L796)

## Overview
A Windows-specific wrapper function that provides locale-aware string formatting for date/time values, converting between different character encodings to ensure proper UTF-8 output.

## Definition

```c
static size_t
strftime_win32(char *dst, size_t dstlen,
			   const char *format, const struct tm *tm)
```
## Detailed Description
The  function addresses encoding issues specific to Windows platforms where the standard  function returns output in CP_ACP encoding (the default operating system codepage), which often differs from PostgreSQL's SERVER_ENCODING. This is particularly problematic for Japanese Windows systems that use SJIS encoding, which PostgreSQL doesn't support as a server encoding.

The function works by:
1. Converting the ASCII format string to wide characters (UTF-16) using 
2. Using  to format the date/time in wide characters
3. Converting the result back to UTF-8 using 
4. Ensuring the output is null-terminated

This approach ensures that locale-aware date/time strings are properly encoded in UTF-8, which PostgreSQL can handle directly.

## Parameters / Member Variables
- `*dst`: Output buffer where the formatted string will be stored
- `dstlen`: Size of the destination buffer
- `*format`: Format string specifying how to format the date/time (expected to be plain ASCII/UTF-8)
- `*tm`: Pointer to a tm structure containing the date/time to format
## Dependencies
- Functions called/Symbols referenced:
  -  (Windows API)
  -  (Windows C runtime)
  -  (Windows API)
  -  (Windows API)
  -  (PostgreSQL logging)
  -  (PostgreSQL macro)
  -  (constant)
- Called from (representative examples):
  - Used as a  replacement for  on Windows platforms

## Notes and Other Information
- This function is Windows-specific and only compiled on Windows builds
- The function assumes format strings are plain ASCII, which is sufficient for PostgreSQL's internal usage
- Uses a fixed-size buffer  as the formats used need only 3 characters
- Returns 0 on failure (when wcsftime fails or conversion fails)
- The  macro redirects all strftime calls in the file to this function
- Does not affect  calls elsewhere in the backend, which are not locale-aware

## Simplified Source

```c
static size_t strftime_win32(char *dst, size_t dstlen,
                            const char *format, const struct tm *tm) {
    wchar_t wformat[8];        // Wide-char format buffer
    wchar_t wbuf[MAX_L10N_DATA];  // Wide-char result buffer

    // Convert ASCII format string to wide characters (UTF-16)
    size_t len = MultiByteToWideChar(CP_UTF8, 0, format, -1,
                                     wformat, lengthof(wformat));
    if (len == 0) {
        elog(ERROR, "could not convert format string from UTF-8: error code %lu",
             GetLastError());
    }

    // Format date/time using wide-character strftime
    len = wcsftime(wbuf, MAX_L10N_DATA, wformat, tm);
    if (len == 0) {
        return 0;  // Formatting failed
    }

    // Convert wide-char result back to UTF-8
    len = WideCharToMultiByte(CP_UTF8, 0, wbuf, len, dst, dstlen - 1,
                              NULL, NULL);
    if (len == 0) {
        elog(ERROR, "could not convert string to UTF-8: error code %lu",
             GetLastError());
    }

    dst[len] = '\0';  // Null-terminate result
    return len;
}
```