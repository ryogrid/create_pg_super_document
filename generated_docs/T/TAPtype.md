# TAPtype

## Location
src/test/regress/pg_regress.c: 95 - 161

## Overview
TAPtype is an enumeration that defines different types of Test Anything Protocol (TAP) output messages used in PostgreSQL regression testing framework to categorize and format test output messages.

## Definition
```c
typedef enum TAPtype
{
    DIAG = 0,
    BAIL,
    NOTE,
    NOTE_DETAIL,
    NOTE_END,
    TEST_STATUS,
    PLAN,
    NONE,
} TAPtype;
```

## Detailed Description
The TAPtype enum serves as a classification system for different types of output in PostgreSQL's regression testing framework (pg_regress). It is primarily used by the TAP (Test Anything Protocol) output formatting functions to determine how messages should be formatted and where they should be directed (stdout vs stderr).

The enum values control the formatting behavior in the emit_tap_output functions, determining whether messages get prefixed with "# " for TAP protocol compliance, whether they go to stdout or stderr, and how multi-line notes are handled. This ensures that PostgreSQL test output conforms to the TAP specification, making it compatible with TAP harnesses and test runners like prove.

## Parameters / Member Variables
- `DIAG`: Diagnostic messages that are printed to stderr with "# " prefix for debugging purposes
- `BAIL`: Critical error messages that cause test termination, printed to stderr with "# " prefix and followed by "Bail out!" to stdout
- `NOTE`: Regular informational messages printed to stdout with "# " prefix
- `NOTE_DETAIL`: Continuation of NOTE messages without "# " prefix (except for the first line), allows multi-line notes
- `NOTE_END`: Marks the end of a NOTE_DETAIL sequence, simply prints a newline
- `TEST_STATUS`: Test result status messages (typically for individual test outcomes)
- `PLAN`: TAP protocol plan messages (e.g., "1..N" indicating test count)
- `NONE`: No special formatting or handling required

## Dependencies
- Functions called/Symbols referenced:
  - Used by emit_tap_output function at src/test/regress/pg_regress.c:151
  - Used by emit_tap_output_v function at src/test/regress/pg_regress.c:152

- Called from (representative examples):
  - emit_tap_output function at src/test/regress/pg_regress.c:330
  - emit_tap_output_v function at src/test/regress/pg_regress.c:340

## Notes and Other Information
- This enum is specific to PostgreSQL's regression testing framework and implements TAP (Test Anything Protocol) compliance
- The DIAG and BAIL types direct output to stderr to ensure visibility under test harnesses like prove
- The NOTE_DETAIL mechanism allows for multi-line diagnostic output while maintaining proper TAP formatting
- The enum values control a global state variable `in_note` to track multi-line note sequences
- Part of PostgreSQL's comprehensive testing infrastructure that ensures database functionality across different platforms and configurations