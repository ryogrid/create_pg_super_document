# syntax_error

## Location
[src/bin/pgbench/pgbench.c:5514-5549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5514-L5549)

## Overview
Reports syntax errors encountered while parsing pgbench script commands with detailed location information and terminates the program execution.

## Definition


## Detailed Description
The syntax_error function provides comprehensive error reporting for syntax errors found in pgbench scripts. It formats and displays detailed error messages including the source file or script name, line number, error message, and optional contextual information. The function constructs a formatted error message using PostgreSQL's PQExpBuffer utilities and outputs it through the logging system. After displaying the error information, it terminates the program with exit code 1, following the fail-fast principle for syntax errors.

When a line of code is provided, the function displays the problematic line and marks the exact error location with a caret (^) character, making it easier for users to identify and fix syntax issues.

## Parameters / Member Variables
- : Source identifier (filename or builtin-script ID) where the error occurred
- : Line number within the script (1-based counting)
- : Complete line containing the syntax error (optional, can be NULL)
- : Name of the backslash command that caused the error (optional, can be NULL)
- : The primary error message describing what went wrong
- : Additional optional error context or explanation (optional, can be NULL)
- : Zero-based column number where the error occurred, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md): Buffer structure for building formatted strings
  - initPQExpBuffer: Initializes the string buffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Formats the primary error message
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md): Appends additional error context
  - termPQExpBuffer: Cleans up the buffer resources
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