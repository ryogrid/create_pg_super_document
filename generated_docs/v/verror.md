# verror

## Location
[src/timezone/zic.c:488-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L488-L503)

## Overview
A variadic error reporting function in the PostgreSQL timezone compiler that formats and displays error messages with file and line context.

## Definition
```c
static void
verror(const char *string, va_list args)
```

## Detailed Description
The `verror` function is the core error formatting and output mechanism in the zic timezone compiler. It takes a format string and variable arguments (via va_list) and produces formatted error messages that include contextual information about file locations and rule contexts.

The function outputs errors in a format compatible with the "cc" compiler to enable integration with BSD error processing tools. It displays the primary file context and, when available, additional rule file context to help users understand the source and context of errors. The output includes proper internationalization support through the _(.) macro.

## Parameters / Member Variables
- `string`: Printf-style format string for the error message
- `args`: Variable arguments list containing values to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - vfprintf

- Called from (representative examples):
  - [error](../e/error.md)  
  - [warning](../w/warning.md)

## Notes and Other Information
- This function is static, meaning it's only accessible within src/timezone/zic.c
- Uses global variables (filename, linenum, rfilename, rlinenum) set by eat/eats functions
- Output format is designed to be compatible with BSD error processing tools
- Provides dual-context error reporting: primary file and rule file contexts
- Uses internationalization macros (_(.)) for error message localization
- Forms the foundation for both error() and warning() functions in the timezone compiler
- The format specifically matches "cc" compiler output for tool integration

## Simplified Source

```c
static void verror(const char *string, va_list args) {
    // Print filename and line number if available
    if (filename)
        fprintf(stderr, _("\"%s\", line %d: "), filename, linenum);

    // Print the formatted error message
    vfprintf(stderr, string, args);

    // Print rule file context if available
    if (rfilename != NULL)
        fprintf(stderr, _(" (rule from \"%s\", line %d)"), rfilename, rlinenum);

    // End with newline
    fprintf(stderr, "\n");
}
```