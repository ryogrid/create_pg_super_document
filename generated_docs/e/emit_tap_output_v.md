# emit_tap_output_v

## Location
[src/test/regress/pg_regress.c:340-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L340-L431)

## Overview
Core function that formats and outputs TAP protocol messages to appropriate streams with proper prefixing, errno handling, and dual output to both console and log file.

## Definition
static void emit_tap_output_v(TAPtype type, const char *fmt, va_list argp)

## Detailed Description
This is the foundational function for all TAP (Test Anything Protocol) output in the PostgreSQL regression testing framework. It handles the complex logic of routing different types of TAP messages to the appropriate output streams (stdout or stderr), applying proper TAP protocol prefixing, managing multi-line note output state, and ensuring consistent dual output to both console and log file when logging is enabled.

The function implements several sophisticated features:
- **Stream routing**: Diagnostic and bail messages go to stderr for visibility under test harnesses, while other messages go to stdout
- **Protocol compliance**: Non-protocol output (diagnostics, notes, bail messages) are prefixed with '#' per TAP specification  
- **Errno preservation**: Saves and restores errno around internal fprintf calls to ensure user format strings with %m placeholders work correctly
- **State management**: Tracks multi-line note output state via the in_note global variable
- **Dual output**: Copies all output to a log file when logging is enabled
- **Special handling**: NOTE_END type immediately outputs newline and returns without further processing

## Parameters / Member Variables  
- `type`: TAPtype enum value specifying the message type (DIAG, BAIL, NOTE, NOTE_DETAIL, NOTE_END, TEST_STATUS, PLAN, NONE)
- `fmt`: Printf-style format string for the output message  
- `argp`: va_list containing the variable arguments for the format string

## Dependencies
- Functions called/Symbols referenced:
  - vfprintf
  - fprintf
  - va_copy
  - in_note (global static bool)
  - logfile (global static FILE*)
  - [TAPtype](../T/TAPtype.md) enum values
- Called from (representative examples):
  - [emit_tap_output](emit_tap_output.md)
  - [bail_out](../b/bail_out.md) (via emit_tap_output)

## Notes and Other Information
- Uses pg_attribute_printf(2, 0) attribute indicating format string is 2nd parameter and no variadic args
- The va_copy operation is necessary to allow printing to both console and log file from the same va_list
- The in_note state variable tracks whether we're in the middle of a multi-line note to avoid duplicate '#' prefixes
- errno preservation is critical for format strings containing %m (which expands to strerror(errno))
- NOTE_END handling allows for clean termination of multi-line diagnostic output
- This function is the single point where all TAP protocol formatting rules are enforced
- The dual output mechanism ensures test results are captured in log files for later analysis

## Simplified Source

```c
static void emit_tap_output_v(TAPtype type, const char *fmt, va_list argp) {
    va_list argp_logfile;
    FILE *fp;
    int save_errno = errno;  // Preserve errno for %m placeholders

    // Route output: DIAG/BAIL to stderr, others to stdout
    if (type == DIAG || type == BAIL)
        fp = stderr;
    else
        fp = stdout;

    // Handle NOTE_END: just print newline and return
    if (type == NOTE_END) {
        in_note = false;
        fprintf(fp, "\n");
        if (logfile)
            fprintf(logfile, "\n");
        return;
    }

    // Copy va_list for dual output to console and logfile
    va_copy(argp_logfile, argp);

    // Prefix non-protocol output with '#' per TAP spec
    if ((type == NOTE || type == DIAG || type == BAIL) ||
        (type == NOTE_DETAIL && !in_note)) {
        fprintf(fp, "# ");
        if (logfile)
            fprintf(logfile, "# ");
    }

    // Output formatted message to both destinations
    errno = save_errno;
    vfprintf(fp, fmt, argp);
    if (logfile) {
        errno = save_errno;
        vfprintf(logfile, fmt, argp_logfile);
    }

    // Track multi-line note state
    if (type == NOTE_DETAIL)
        in_note = true;

    // BAIL messages need additional protocol output
    if (type == BAIL) {
        fprintf(stdout, "Bail out!");
        if (logfile)
            fprintf(logfile, "Bail out!");
    }

    va_end(argp_logfile);

    // Add newline for most message types
    if (type != NOTE_DETAIL) {
        fprintf(fp, "\n");
        if (logfile)
            fprintf(logfile, "\n");
    }
    fflush(NULL);
}
```