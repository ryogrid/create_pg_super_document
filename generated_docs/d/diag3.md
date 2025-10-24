# diag3

## Location
[src/tools/pg_bsd_indent/io.c:573-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L573-L589)

## Overview
A diagnostic message output function in the pg_bsd_indent tool that formats and prints error or warning messages with a single integer argument.

## Definition
```c
void diag3(int level, const char *msg, int a)
```

## Detailed Description
The `diag3` function is a diagnostic output utility specifically designed for the pg_bsd_indent tool (PostgreSQL's BSD-style code indentation utility). It formats and outputs diagnostic messages that include one integer argument. The function handles both error and warning messages, directing output appropriately based on the current output stream configuration.

When the output stream is stdout (indicating that formatted code is being written to stdout), diagnostic messages are embedded as special comments in the output stream with the format "/**INDENT** [Warning|Error]@line: message */". When output is directed elsewhere, messages are sent to stderr in a standard format.

The function also manages error state by setting the global `found_err` flag when error-level messages are reported.

## Parameters / Member Variables
- `level`: Integer indicating message severity (0 for warnings, non-zero for errors)
- `msg`: Format string for the diagnostic message (expects one integer format specifier)
- `a`: Integer argument to be inserted into the message format string

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard library)
- Called from (representative examples):
  - [main](../m/main.md) (from indent.c at lines 538, 593, 811)

## Notes and Other Information
- Part of the pg_bsd_indent tool's diagnostic system
- Sets global `found_err` flag to 1 when reporting errors
- Uses special comment formatting when output stream is stdout to avoid interfering with formatted code
- Companion to `diag2` function which handles messages without integer arguments
- The function assumes the format string `msg` contains exactly one integer format specifier

## Simplified Source

```c
void diag3(int level, const char *msg, int a) {
    // Mark error if level is non-zero
    if (level)
        found_err = 1;

    // Format message prefix
    const char *prefix = level == 0 ? "Warning" : "Error";

    // Output to stdout as INDENT comment or stderr as standard message
    if (output == stdout) {
        fprintf(stdout, "/**INDENT** %s@%d: ", prefix, line_no);
        fprintf(stdout, msg, a);
        fprintf(stdout, " */\n");
    } else {
        fprintf(stderr, "%s@%d: ", prefix, line_no);
        fprintf(stderr, msg, a);
        fprintf(stderr, "\n");
    }
}
```