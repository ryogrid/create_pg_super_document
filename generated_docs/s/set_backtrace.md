# set_backtrace

## Location
[src/backend/utils/error/elog.c:1116-1156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1116-L1156)

## Overview
Captures call stack backtrace information and attaches it to PostgreSQL error data for debugging purposes.

## Definition
```c
static void set_backtrace(ErrorData *edata, int num_skip)
```

## Detailed Description
This static function is the core implementation for capturing call stack backtraces in PostgreSQL's error reporting system. It uses system-provided backtrace facilities (when available) to capture the current execution stack, formats it into a human-readable string, and attaches it to the error data structure. The function supports skipping a specified number of inner stack frames to avoid showing internal backtrace support functions in the output. When backtrace support is not available at compile time, it provides a fallback message indicating that backtrace generation is not supported.

## Parameters / Member Variables
- `edata`: Pointer to ErrorData structure where backtrace will be stored
- `num_skip`: Number of innermost stack frames to skip in the backtrace output
- Return value: void (no return value)

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (struct type for error information)
  - lengthof (macro for array length calculation)
  - [initStringInfo](../i/initStringInfo.md) (string buffer initialization)
  - [appendStringInfo](../a/appendStringInfo.md) (formatted string append)
  - [appendStringInfoString](../a/appendStringInfoString.md) (string append)
  - backtrace (system function for stack capture - [when](../w/when.md) HAVE_BACKTRACE_SYMBOLS defined)
  - backtrace_symbols (system function for symbol resolution - [when](../w/when.md) HAVE_BACKTRACE_SYMBOLS defined)
- Called from (representative examples):
  - [errfinish](../e/errfinish.md) (error finalization function)
  - [errbacktrace](../e/errbacktrace.md) (public backtrace interface)

## Notes and Other Information
- Static function - only accessible within elog.c
- Conditional compilation based on HAVE_BACKTRACE_SYMBOLS availability
- Captures up to 100 stack frames when backtrace support is available
- Each frame is formatted with newline prefix for readability
- Properly manages memory by freeing backtrace_symbols result
- Stores final backtrace string in edata->backtrace field
- Gracefully handles cases where backtrace_symbols returns NULL
- Requires that this function and related functions are not inlined for accurate backtraces
- Located in src/backend/utils/error/elog.c:1116-1156

## Simplified Source

```c
static void
set_backtrace(ErrorData *edata, int num_skip)
{
    StringInfoData errtrace;

    initStringInfo(&errtrace);

#ifdef HAVE_BACKTRACE_SYMBOLS
    {
        void *buf[100];
        int nframes;
        char **strfrms;

        // Capture call stack
        nframes = backtrace(buf, lengthof(buf));
        strfrms = backtrace_symbols(buf, nframes);

        if (strfrms != NULL) {
            // Format stack frames, skipping internal frames
            for (int i = num_skip; i < nframes; i++)
                appendStringInfo(&errtrace, "\n%s", strfrms[i]);
            free(strfrms);
        }
    }
#else
    appendStringInfoString(&errtrace,
        "backtrace generation is not supported by this installation");
#endif

    edata->backtrace = errtrace.data;
}
```