# tsearch_readline_callback

## Location
[src/backend/tsearch/ts_locale.c:225-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_locale.c#L225-L252)

## Overview
An error context callback function that provides detailed error location information when errors occur while reading text search configuration files.

## Definition

```c
static void
tsearch_readline_callback(void *arg)
```
## Detailed Description
This static function serves as an error context callback that is registered with PostgreSQL's error reporting system when reading text search configuration files. When an error occurs during file processing, this callback automatically adds contextual information to the error message, including the filename and line number where the error occurred.

The function handles two scenarios: when a current line is available (and safe to display), it includes both the line number and the actual line content in the error context; when no safe line content is available (such as during encoding errors), it provides only the filename and line number.

## Parameters / Member Variables
- `*arg`: Generic void pointer that is cast to tsearch_readline_state pointer containing file reading state information
## Dependencies
- Functions called/Symbols referenced:
  - errcontext
  - [tsearch_readline_state](tsearch_readline_state.md) (struct type cast)
- Called from (representative examples):
  - [tsearch_readline_begin](tsearch_readline_begin.md) (registers this callback)

## Notes and Other Information
- Declared as static function, only used within ts_locale.c
- Registered as callback via error_context_stack in tsearch_readline_begin()
- Provides two different error message formats depending on whether current line data is safe to display
- Avoids displaying line content when encoding errors might be present (safety measure)
- Essential for providing meaningful error messages with precise location information in configuration files
- Automatically invoked by PostgreSQL's error reporting system when ereport() is called
- Part of PostgreSQL's error context callback mechanism for enhanced error diagnostics