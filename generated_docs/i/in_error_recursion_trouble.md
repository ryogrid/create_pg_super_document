# in_error_recursion_trouble

## Location
[src/backend/utils/error/elog.c:297-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L297-L308)

## Overview
Detects whether the system is at risk of infinite error recursion by checking if the error handling recursion depth exceeds a safe threshold.

## Definition
```c
bool in_error_recursion_trouble(void)
```

## Detailed Description
This function provides a centralized mechanism to detect when the PostgreSQL error handling system is at risk of infinite recursion. It monitors the static `recursion_depth` variable that tracks how many levels deep the error handling system has recursed. When this depth exceeds 2, the function returns true, indicating that fallback error handling measures should be taken to prevent system instability. This is critical for preventing stack overflow crashes when error handling code itself triggers additional errors. The threshold of 2 allows for one level of normal error recovery while still catching dangerous recursive scenarios early.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - recursion_depth (static global variable)
- Called from (representative examples):
  - [err_gettext](../e/err_gettext.md)
  - [errstart](../e/errstart.md)
  - EVALUATE_MESSAGE
  - EVALUATE_MESSAGE_PLURAL
  - [write_eventlog](../w/write_eventlog.md)
  - [write_console](../w/write_console.md)
  - [err_sendstring](../e/err_sendstring.md)
  - LOG_DESTINATION_JSONLOG

## Notes and Other Information
- The function uses a simple threshold check: recursion_depth > 2 indicates trouble
- The recursion_depth variable is incremented/decremented throughout the error handling subsystem to track nesting levels
- This function enables various fallback strategies when infinite recursion is detected, such as simplified error reporting or emergency shutdown procedures
- Critical for system stability - prevents stack overflow crashes during error cascades
- The threshold of 2 levels is conservative but allows for normal error recovery patterns while catching dangerous recursion early
- Part of PostgreSQL's robust error handling infrastructure designed to handle error-on-error scenarios gracefully
- When this function returns true, callers typically switch to simplified, non-recursive error handling modes

## Simplified Source

```c
// Simplified version of in_error_recursion_trouble
bool in_error_recursion_trouble(void) {
    // Check if error handling has recursed too deeply
    // Return true if recursion depth exceeds safe threshold (2 levels)
    return (recursion_depth > 2);
}
```

Key simplifications made:
- Added descriptive comments explaining the core logic
- The function is already quite simple, so minimal changes were needed
- Preserved the essential threshold check that prevents infinite recursion
- Focused on the main safety mechanism: depth > 2 indicates trouble