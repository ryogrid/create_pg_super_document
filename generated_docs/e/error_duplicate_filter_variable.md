# error_duplicate_filter_variable

## Location
[src/backend/commands/event_trigger.c:261-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L261-L272)

## Overview
Reports a syntax error when a filter variable is specified more than once in an event trigger definition.

## Definition
```c
static void error_duplicate_filter_variable(const char *defname)
```

## Detailed Description
error_duplicate_filter_variable is a simple static helper function that provides a standardized error message when duplicate filter variables are detected during event trigger creation. It uses the PostgreSQL error reporting system to generate a SYNTAX_ERROR with a clear message indicating which filter variable was duplicated. This function helps maintain consistency in error reporting and provides users with clear feedback about what went wrong in their event trigger definition.

## Parameters / Member Variables
- `defname`: The name of the filter variable that was specified more than once (e.g., "tag")

## Dependencies
- Functions called/Symbols referenced:
  - ereport() - PostgreSQL's error reporting function
  - ERRCODE_SYNTAX_ERROR - [error](error.md) code constant for syntax errors
- Called from (representative examples):
  - [CreateEventTrigger](../C/CreateEventTrigger.md)() - when duplicate filter variables are detected in WHEN clauses

## Notes and Other Information
- This is a static function only accessible within event_trigger.c
- Provides consistent error messaging for duplicate filter variable detection
- Currently, "tag" is the primary filter variable that can be duplicated
- Uses ERRCODE_SYNTAX_ERROR which is appropriate for malformed SQL statements
- Part of the input validation infrastructure for event trigger creation
- The function does not return as ereport(ERROR, ...) throws an exception that unwinds the call stack
- Helps users identify and fix syntax errors in their CREATE EVENT TRIGGER statements