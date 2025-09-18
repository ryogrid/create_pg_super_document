# errcontext_msg

## Location
src/backend/utils/error/elog.c: 1365 - 1390

## Overview
A function that adds context error message text to the current error, supporting multiple calls to build up a stack of context information.

## Definition
```c
int errcontext_msg(const char *fmt, ...)
```

## Detailed Description
This function is part of PostgreSQL's error reporting system and provides a unique capability among the error message functions: it allows multiple calls to build up a stack of context information. Unlike other error message functions that typically replace previous content, errcontext_msg accumulates context information with each call, where earlier calls represent more-closely-nested states.

The function operates on the current error context and uses the context_domain field rather than the regular domain field, indicating its special role in building contextual error information. This is particularly useful for tracking the call stack or nested operations when an error occurs.

## Parameters / Member Variables
- `fmt`: Format string for the context message
- `...`: Variable arguments that correspond to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
  - EVALUATE_MESSAGE (macro)
  - MemoryContextSwitchTo
- Called from (representative examples):
  - errcontext (macro in elog.h, multiple definitions)

## Notes and Other Information
- Returns 0 as the return value does not matter for this function
- Unique among error message functions in allowing multiple calls to build up context stack
- Uses context_domain field instead of regular domain field for special context handling
- Earlier calls represent more-closely-nested states in the context stack
- Part of PostgreSQL's error reporting infrastructure for providing execution context
- Manages recursion depth and memory context for safe operation
- Typically accessed through the errcontext() macro rather than called directly