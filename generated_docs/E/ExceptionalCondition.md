# ExceptionalCondition

## Location
[src/backend/utils/error/assert.c:30-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/assert.c#L30-L67)

## Overview
ExceptionalCondition is a low-level function that handles the failure of Assert() conditions in PostgreSQL, providing diagnostic output and terminating the process when assertions fail.

## Definition

```c
void
ExceptionalCondition(const char *conditionName,
					 const char *fileName,
					 int lineNumber)
```
## Detailed Description
ExceptionalCondition is the core assertion failure handler in PostgreSQL. It is intentionally designed to bypass the normal error reporting infrastructure (elog) to minimize dependencies and ensure that assertion failures can be reported even when the system is in a compromised state. The function outputs detailed diagnostic information to stderr, including the failed assertion condition, source file location, line number, and process ID. It also provides optional features like backtrace generation and debugger attachment support before ultimately terminating the process with abort().

## Parameters / Member Variables
- `*conditionName`: The string representation of the failed assertion condition
- `*fileName`: The source file name where the assertion failed
- `lineNumber`: The line number in the source file where the assertion failed
## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (parameter validation)
  - [write_stderr](../w/write_stderr.md) (error output)
  - lengthof (array length calculation)
  - getpid (process ID retrieval)
  - fflush (output flushing)
  - backtrace/backtrace_symbols_fd (optional backtrace generation)
  - sleep (optional debugger attachment delay)
  - abort (process termination)

- Called from (representative examples):
  - Assert (assertion macro)
  - AssertMacro (assertion macro variant)
  - AssertPointerAlignment (pointer alignment assertion)
  - [pg_re_throw](../p/pg_re_throw.md) (exception re-throwing context)

## Notes and Other Information
- The function intentionally avoids using elog() to minimize infrastructure dependencies during assertion failures
- Includes conditional compilation support for backtrace generation (HAVE_BACKTRACE_SYMBOLS)
- Supports optional sleep for debugger attachment (SLEEP_ON_ASSERT configuration)
- Always terminates the process with abort() after reporting the failure
- Validates input parameters to handle cases where the assertion system itself may be compromised
- Critical for PostgreSQL's debugging and development infrastructure, ensuring assertion failures are properly diagnosed

## Simplified Source

```c
void ExceptionalCondition(const char *conditionName,
                         const char *fileName,
                         int lineNumber)
{
    // Report assertion failure to stderr
    if (!PointerIsValid(conditionName) || !PointerIsValid(fileName))
        write_stderr("TRAP: ExceptionalCondition: bad arguments in PID %d\n",
                     (int) getpid());
    else
        write_stderr("TRAP: failed Assert(\"%s\"), File: \"%s\", Line: %d, PID: %d\n",
                     conditionName, fileName, lineNumber, (int) getpid());

    // Ensure message is output
    fflush(stderr);

    // Optional: dump backtrace if supported
    #ifdef HAVE_BACKTRACE_SYMBOLS
    {
        void *buf[100];
        int nframes = backtrace(buf, lengthof(buf));
        backtrace_symbols_fd(buf, nframes, fileno(stderr));
    }
    #endif

    // Optional: sleep for debugger attachment
    #ifdef SLEEP_ON_ASSERT
    sleep(1000000);
    #endif

    // Terminate the process
    abort();
}
```