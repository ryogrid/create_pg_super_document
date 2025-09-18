# defaultNoticeProcessor

## Location
[src/interfaces/libpq/fe-connect.c:7376-7387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7376-L7387)

## Overview
The default notice message processor function that outputs PostgreSQL notice messages to stderr, providing a simple default behavior for client applications.

## Definition
```c
static void defaultNoticeProcessor(void *arg, const char *message)
```

## Detailed Description
This function serves as the default implementation for processing notice messages in libpq client applications. It provides a simple behavior of printing notice messages directly to the standard error stream (stderr). This function is the second level in libpq's two-level notice handling system, receiving processed notice text from the notice receiver.

Applications can override this default behavior by providing their own notice processor function through libpq's notice handling API. This allows applications to redirect notices to application-specific destinations such as log files, GUI windows, or other output mechanisms.

## Parameters / Member Variables
- `arg`: A void pointer argument that is not used in this implementation (marked as unused)
- `message`: A const char pointer containing the notice message text to be processed. The function expects this string to already include a trailing newline character

## Dependencies
- Functions called/Symbols referenced:
  - No external PostgreSQL functions referenced (uses standard C library fprintf)
- Called from (representative examples):
  - internalPQconninfoOption (fe-connect.c:437)
  - [pqMakeEmptyPGconn](../p/pqMakeEmptyPGconn.md) (fe-connect.c:4572)

## Notes and Other Information
- This function is marked as static, indicating it's only used within the fe-connect.c file
- The function uses fprintf to output directly to stderr
- The message is expected to already contain a newline character at the end
- Applications should not simply discard notices as they may contain important diagnostic information
- This is part of libpq's extensible notice handling system that allows applications to customize notice processing
- The function serves as a reasonable default that ensures notices are visible to users during development and debugging