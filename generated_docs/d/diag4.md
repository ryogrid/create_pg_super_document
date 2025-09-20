# diag4

## Location
[src/tools/pg_bsd_indent/io.c:556-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L556-L572)

## Overview
Outputs diagnostic messages (warnings and errors) with formatted output that includes line numbers and two integer parameters.

## Definition

```c
void
diag4(int level, const char *msg, int a, int b)
```
## Detailed Description
The  function is a diagnostic reporting utility in the PostgreSQL BSD indent tool that outputs formatted warning and error messages. The function handles message output differently depending on whether the output is being written to stdout or another destination.

When output is directed to stdout, diagnostic messages are formatted as special INDENT comments (/**INDENT** format) that get embedded in the output stream. This allows the messages to be visible in the formatted code without breaking the syntax. When output goes elsewhere, messages are written to stderr in a standard format.

The function supports parameterized messages by accepting two integer parameters (a and b) that get substituted into the message format string. This allows for flexible diagnostic reporting with context-specific information like line numbers, column positions, or other relevant values.

The level parameter determines whether the message is treated as a warning (level 0) or error (level != 0), with errors setting the global  flag to indicate that problems were encountered during processing.

## Parameters / Member Variables
- : Diagnostic level (0 for warning, non-zero for error)
- : Format string for the diagnostic message (similar to printf format)
- : First integer parameter for message formatting
- : Second integer parameter for message formatting

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function for formatted output
  - Uses global variables: , , 
- Called from (representative examples):
  - Referenced in  header file, indicating use throughout the indent tool

## Notes and Other Information
- Part of the diagnostic reporting system for the indent tool
- Supports both warning and error reporting with different output formatting
- When writing to stdout, messages are embedded as /**INDENT** comments to maintain code syntax
- When writing to other outputs, messages go to stderr in standard format
- The  global flag tracks whether any errors have occurred during processing
- Uses the current  to provide context for where issues were detected
- The dual-parameter design allows for flexible message formatting with numeric context
- Essential for providing user feedback about formatting issues and problems in the input code