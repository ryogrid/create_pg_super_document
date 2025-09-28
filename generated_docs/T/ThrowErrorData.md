# ThrowErrorData

## Location
[src/backend/utils/error/elog.c:1892-1950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1892-L1950)

## Overview
Re-reports an error described by an ErrorData structure, allowing for error propagation from background workers to main processes with level flexibility.

## Definition
```c
void ThrowErrorData(ErrorData *edata)
```

## Detailed Description
ThrowErrorData provides a mechanism to re-report errors using information from an existing ErrorData structure. Unlike ReThrowError which assumes ERROR level, this function supports any error level specified in the ErrorData structure. The function starts a new error reporting cycle using errstart(), copies all relevant fields from the provided ErrorData into the current error stack entry, and then completes the error processing with errfinish(). Boolean flags such as output_to_server are computed using default rules rather than being inherited from the input ErrorData. This function is primarily designed for propagating errors from background worker processes to their responsible backend processes, potentially with modifications during the propagation.

## Parameters / Member Variables
- `edata`: Pointer to ErrorData structure containing the error information to be re-reported

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (structure type)
  - [errstart](../e/errstart.md) (error reporting initiation)
  - [errfinish](../e/errfinish.md) (error reporting completion)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
  - errordata (global array)
  - errordata_stack_depth (global variable)
  - recursion_depth (global variable)

- Called from (representative examples):
  - [HandleParallelMessage](../H/HandleParallelMessage.md)

## Notes and Other Information
- Supports all error levels, not just ERROR like ReThrowError
- Copies string fields using pstrdup() to ensure proper memory management
- Does not copy the message_id field (explicitly noted as not available)
- Uses default rules for boolean flags rather than copying them from input
- Manages memory context switching to ensure strings are allocated in the correct context
- Primarily used in parallel processing scenarios for error propagation
- Follows the standard error reporting pattern: errstart → field setup → errfinish
- Increments and decrements recursion_depth to track nested error handling

## Simplified Source

```c
// Simplified version of ThrowErrorData
void ThrowErrorData(ErrorData *edata) {
    ErrorData *newedata;
    MemoryContext oldcontext;

    // Start error reporting - exit early if error level is filtered
    if (!errstart(edata->elevel, edata->domain))
        return;

    // Get current error stack entry and setup context
    newedata = &errordata[errordata_stack_depth];
    recursion_depth++;
    oldcontext = MemoryContextSwitchTo(newedata->assoc_context);

    // Copy error fields from input ErrorData
    if (edata->sqlerrcode != 0)
        newedata->sqlerrcode = edata->sqlerrcode;
    if (edata->message)
        newedata->message = pstrdup(edata->message);
    if (edata->detail)
        newedata->detail = pstrdup(edata->detail);
    if (edata->hint)
        newedata->hint = pstrdup(edata->hint);
    if (edata->context)
        newedata->context = pstrdup(edata->context);

    // Copy positional and contextual information
    newedata->cursorpos = edata->cursorpos;
    newedata->internalpos = edata->internalpos;
    if (edata->internalquery)
        newedata->internalquery = pstrdup(edata->internalquery);

    // Restore memory context and complete error reporting
    MemoryContextSwitchTo(oldcontext);
    recursion_depth--;
    errfinish(edata->filename, edata->lineno, edata->funcname);
}
```

Key simplifications made:
- Condensed repetitive field copying logic
- Added explanatory comments for each major phase
- Preserved essential memory management and error reporting flow
- Maintained all critical error field transfers