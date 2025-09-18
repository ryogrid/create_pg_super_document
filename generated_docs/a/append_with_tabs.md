# append_with_tabs

## Location
src/backend/utils/error/elog.c: 3719 - 3737

## Overview
A static utility function that appends a string to a StringInfo buffer while automatically inserting tab characters after newlines for proper indentation formatting.

## Definition
```c
static void append_with_tabs(StringInfo buf, const char *str)
```

## Detailed Description
This function provides formatted string appending specifically designed for PostgreSQL's server-side logging system. It processes the input string character by character, copying each character to the destination buffer using appendStringInfoCharMacro for efficiency. When a newline character is encountered, the function automatically inserts a tab character immediately following it. This creates a consistent indentation pattern that improves readability of multi-line log messages by ensuring continuation lines are properly indented. The function is particularly useful for formatting error details, hints, and context information in server logs where hierarchical structure needs to be visually represented.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted string will be appended
- `str`: Null-terminated C string to be processed and appended with tab formatting

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro (used for efficient character appending)
- Called from (representative examples):
  - send_message_to_server_log (multiple locations for formatting various message components like detail, hint, context, and other error fields)

## Notes and Other Information
This function is specifically designed for server-side logging and is extensively used by send_message_to_server_log to ensure consistent formatting of multi-line error message components. The automatic tab insertion after newlines creates a visually hierarchical structure in log files, making it easier to distinguish between different parts of error messages and to parse log entries manually or programmatically.