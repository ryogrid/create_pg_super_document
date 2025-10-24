# emit_tap_output

## Location
[src/test/regress/pg_regress.c:330-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L330-L339)

## Overview
A variadic wrapper function that formats and outputs TAP protocol messages by forwarding arguments to emit_tap_output_v.

## Definition
static void emit_tap_output(TAPtype type, const char *fmt, ...)

## Detailed Description
This function serves as a convenient variadic interface for generating TAP (Test Anything Protocol) output in the PostgreSQL regression testing framework. It acts as a wrapper around the core emit_tap_output_v function, handling the variadic argument processing by converting the variable argument list into a va_list that can be passed to the underlying implementation.

The function follows the standard C pattern for variadic wrapper functions, using va_start to initialize the argument list, calling the core implementation function, and then cleaning up with va_end. This design allows callers to use a more natural printf-style interface while keeping the complex TAP formatting logic centralized in emit_tap_output_v.

## Parameters / Member Variables
- `type`: TAPtype enum value indicating the type of TAP message to output (DIAG, BAIL, NOTE, TEST_STATUS, etc.)
- `fmt`: Printf-style format string for the output message
- `...`: Variable arguments corresponding to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  - [emit_tap_output_v](emit_tap_output_v.md)
  - va_start (macro)
  - va_end (macro)
- Called from (representative examples):
  - [test_status_print](../t/test_status_print.md)
  - plan() macro
  - note() macro  
  - diag() macro
  - bail() macro

## Notes and Other Information
- Uses the pg_attribute_printf(2, 3) attribute for compile-time format string checking
- The function signature indicates that the format string is the 2nd parameter and variadic args start at the 3rd
- This is the primary interface used throughout the codebase for TAP output generation
- Several convenience macros (plan, note, diag, bail) are defined that wrap calls to this function
- The actual TAP formatting, file output routing, and protocol compliance is handled by emit_tap_output_v

## Simplified Source

```c
static void emit_tap_output(TAPtype type, const char *fmt, ...) {
    va_list argp;

    // Convert variadic args to va_list and forward to core implementation
    va_start(argp, fmt);
    emit_tap_output_v(type, fmt, argp);
    va_end(argp);
}
```