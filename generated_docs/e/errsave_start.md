# errsave_start

## Location
[src/backend/utils/error/elog.c:633-684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L633-L684)

## Overview
Begins a "soft" error-reporting cycle that can capture error details without immediately terminating execution, supporting both traditional error handling and error capture scenarios.

## Definition

```c
bool
errsave_start(struct Node *context, const char *domain)
```
## Detailed Description
errsave_start provides a flexible error reporting mechanism that supports both traditional error handling and "soft" error capture. When no ErrorSaveContext is provided, it delegates to errstart() for normal ERROR processing. When an ErrorSaveContext is provided, it enables capturing error information without immediate process termination.

The function supports two soft error modes:
1. Notification-only mode: Sets error_occurred flag and returns false to skip detailed error processing
2. Detail capture mode: Creates an error stack entry for collecting full error information

This mechanism is particularly useful for operations that need to attempt potentially-failing actions while capturing error details for later handling, such as data validation or conditional operations.

## Parameters / Member Variables
- `*context`: Node pointer that may be an ErrorSaveContext for soft error handling, or NULL for traditional error processing
- `*domain`: Error domain string for categorizing the error source
## Dependencies
- Functions called/Symbols referenced:
  - [errstart](errstart.md)
  - [get_error_stack_entry](../g/get_error_stack_entry.md)
  - [set_stack_entry_domain](../s/set_stack_entry_domain.md)
- Called from (representative examples):
  - errsave_domain
  - ereturn

## Notes and Other Information
- Returns true to continue error processing, false to skip remaining error steps
- Falls back to errstart(ERROR, domain) when context is NULL or not an ErrorSaveContext
- Uses LOG level internally to signal successful soft error handling to errsave_finish
- Allocates error data in CurrentMemoryContext rather than ErrorContext for soft errors
- Sets error_occurred flag in ErrorSaveContext to notify caller of error detection
- Supports details_wanted flag to control whether full error information is collected
- Uses recursion_depth tracking for consistency with standard error handling

## Simplified Source

```c
bool
errsave_start(struct Node *context, const char *domain)
{
    ErrorSaveContext *escontext;
    ErrorData *edata;

    // No soft error context provided - use normal error handling
    if (context == NULL || !IsA(context, ErrorSaveContext))
        return errstart(ERROR, domain);

    // Set error_occurred flag in the context
    escontext = (ErrorSaveContext *) context;
    escontext->error_occurred = true;

    // If caller only wants notification, skip detailed processing
    if (!escontext->details_wanted)
        return false;

    // Set up error stack entry for detailed error info
    recursion_depth++;
    edata = get_error_stack_entry();
    edata->elevel = LOG;  // Signal soft error to errsave_finish
    set_stack_entry_domain(edata, domain);
    edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
    edata->assoc_context = CurrentMemoryContext;
    recursion_depth--;

    return true;  // Continue with error processing
}
```