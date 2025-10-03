# write_console

## Location
[src/backend/utils/error/elog.c:2576-2653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2576-L2653)

## Overview
The write_console function writes PostgreSQL log messages directly to the console (stderr), with platform-specific handling for character encoding conversion on Windows.

## Definition
```c
static void write_console(const char *line, int len)
```

## Detailed Description
This function provides cross-platform console output for PostgreSQL log messages, with special handling for Windows systems:

**Windows Implementation:**
- Attempts UTF-16 conversion using pgwin32_message_to_UTF16() for proper Unicode display
- Uses WriteConsoleW() for UTF-16 output when possible (when stderr is not redirected)
- Falls back to standard write() system call if WriteConsoleW() fails or UTF-16 conversion fails
- Includes safeguards against error recursion and memory allocation issues

**Non-Windows Implementation:**
- Uses standard write() system call directly to stderr
- Notes indicate that encoding conversion for non-Windows platforms is not yet fully implemented

The function is designed to be resilient, ignoring write errors since there's no meaningful way to report console write failures.

## Parameters / Member Variables
- `*line`: The log message string to write to console
- `len`: Length of the message string in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_message_to_UTF16](../p/pgwin32_message_to_UTF16.md) (Windows: PostgreSQL UTF-16 conversion utility)
  - GetStdHandle, WriteConsoleW (Windows: Windows API console functions)
  - write, fileno (POSIX: standard I/O functions)
  - [in_error_recursion_trouble](../i/in_error_recursion_trouble.md) (PostgreSQL error handling)
  - [pfree](../p/pfree.md) (PostgreSQL memory management)
- Called from (representative examples):
  - [send_message_to_server_log](../s/send_message_to_server_log.md)
  - [write_stderr](write_stderr.md)

## Notes and Other Information
- Cross-platform function with conditional compilation (#ifdef WIN32)
- On Windows, automatically detects console redirection and adapts behavior accordingly
- WriteConsoleW() will fail if stderr is redirected to a file, triggering fallback to write()
- Uses binary mode for stderr on Windows (set in SubPostmasterMain())
- Explicitly ignores write() return codes since console write errors cannot be meaningfully handled
- Part of PostgreSQL's platform-specific logging infrastructure
- Future enhancement noted for non-Windows encoding conversion using no-throw version of pg_do_encoding_conversion()
- Located in src/backend/utils/error/elog.c alongside other logging functions
- Provides robust fallback mechanisms to ensure log messages are always written, even if optimal formatting fails

## Simplified Source

```c
// Simplified version of write_console
static void write_console(const char *line, int len) {
#ifdef WIN32
    // Windows: Try UTF-16 conversion and WriteConsoleW() first
    if (can_use_utf16_conversion()) {
        WCHAR *utf16_message = convert_to_utf16(line, len);
        if (utf16_message != NULL) {
            HANDLE console_handle = GetStdHandle(STD_ERROR_HANDLE);
            if (WriteConsoleW(console_handle, utf16_message, utf16_len, &written, NULL)) {
                pfree(utf16_message);
                return; // Success with UTF-16
            }
            pfree(utf16_message); // Cleanup on failure
        }
    }
#endif

    // Fallback: Write directly to stderr (all platforms)
    write(fileno(stderr), line, len);
    // Ignore any write errors - nothing useful we can do about them
}
```

Key simplifications made:
- Abstracted complex Windows condition checks into `can_use_utf16_conversion()`
- Simplified UTF-16 conversion logic with cleaner variable names
- Removed detailed error handling comments for clarity
- Consolidated the main execution flow into two clear paths: UTF-16 attempt and fallback
- Focused on the core algorithm: try Windows-specific UTF-16 output, fallback to standard write
- Removed platform-specific implementation details while preserving the essential logic