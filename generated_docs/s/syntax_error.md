# syntax_error

## Location
[src/bin/pgbench/pgbench.c:5514-5549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5514-L5549)

## Overview
Reports syntax errors encountered while parsing pgbench script commands with detailed location information and terminates the program execution.

## Definition

```c
void
syntax_error(const char *source, int lineno,
			 const char *line, const char *command,
			 const char *msg, const char *more, int column)
```
## Detailed Description
The syntax_error function provides comprehensive error reporting for syntax errors found in pgbench scripts. It formats and displays detailed error messages including the source file or script name, line number, error message, and optional contextual information. The function constructs a formatted error message using PostgreSQL's PQExpBuffer utilities and outputs it through the logging system. After displaying the error information, it terminates the program with exit code 1, following the fail-fast principle for syntax errors.

When a line of code is provided, the function displays the problematic line and marks the exact error location with a caret (^) character, making it easier for users to identify and fix syntax issues.

## Parameters / Member Variables
- `*source`: Source identifier (filename or builtin-script ID) where the error occurred
- `lineno`: Line number within the script (1-based counting)
- `*line`: Complete line containing the syntax error (optional, can be NULL)
- `*command`: Name of the backslash command that caused the error (optional, can be NULL)
- `*msg`: The primary error message describing what went wrong
- `*more`: Additional optional error context or explanation (optional, can be NULL)
- `column`: Zero-based column number where the error occurred, or -1 if unknown
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md): Buffer structure for building formatted strings
  - [initPQExpBuffer](../i/initPQExpBuffer.md): Initializes the string buffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Formats the primary error message
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md): Appends additional error context
  - [termPQExpBuffer](../t/termPQExpBuffer.md): Cleans up the buffer resources
  - pg_log_error: Outputs the formatted error message
  - fprintf: Outputs the problematic line and error marker to stderr
  - exit: Terminates the program with error code 1
- Called from (representative examples):
  - [process_backslash_command](../p/process_backslash_command.md): Main command processor that validates backslash commands
  - [string_to_uuid](string_to_uuid.md): UUID parsing functions that detect malformed UUID strings

## Notes and Other Information
- This function never returns as it always calls exit(1) after reporting the error
- The function is specifically designed for pgbench script parsing errors, not SQL syntax errors
- Column-based error reporting provides precise error location when line information is available
- The error format follows standard compiler-like error reporting conventions (source:line: message)
- Memory cleanup is performed via termPQExpBuffer before program termination
- The function handles various optional parameters gracefully, constructing appropriate error messages based on available information

## Simplified Source

```c
void syntax_error(const char *source, int lineno,
                 const char *line, const char *command,
                 const char *msg, const char *more, int column)
{
    PQExpBufferData buf;

    initPQExpBuffer(&buf);

    // Build error message with source:line: format
    printfPQExpBuffer(&buf, "%s:%d: %s", source, lineno, msg);

    // Add optional context information
    if (more != NULL)
        appendPQExpBuffer(&buf, " (%s)", more);
    if (column >= 0 && line == NULL)
        appendPQExpBuffer(&buf, " at column %d", column + 1);
    if (command != NULL)
        appendPQExpBuffer(&buf, " in command \"%s\"", command);

    pg_log_error("%s", buf.data);
    termPQExpBuffer(&buf);

    // Show problematic line with error marker if available
    if (line != NULL) {
        fprintf(stderr, "%s\n", line);
        if (column >= 0)
            fprintf(stderr, "%*c error found here\n", column + 1, '^');
    }

    exit(1);
}
```