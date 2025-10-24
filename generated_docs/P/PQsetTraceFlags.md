# PQsetTraceFlags

## Location
[src/interfaces/libpq/fe-trace.c:64-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L64-L79)

## Overview
Sets the trace flags for an active tracing session to control the format and content of the trace output.

## Definition
```c
void PQsetTraceFlags(PGconn *conn, int flags)
```

## Detailed Description
PQsetTraceFlags allows fine-grained control over the behavior of protocol tracing after tracing has been enabled with PQtrace. The function accepts bitwise flags that modify how trace output is formatted and what information is included. It only operates on connections that have tracing already enabled; if tracing is not active (Pfdebug is NULL), the function does nothing. This design ensures that trace flags are only set when they can actually affect output.

## Parameters / Member Variables
- `conn`: The PostgreSQL connection handle (PGconn *) for which trace flags should be set
- `flags`: Integer containing bitwise flags that control trace output behavior. Available flags include:
  - PQTRACE_SUPPRESS_TIMESTAMPS (1<<0): Suppress timestamp output in trace messages
  - PQTRACE_REGRESS_MODE (1<<1): Enable regression test mode for consistent output

## Dependencies
- Functions called/Symbols referenced:
  - (none - direct field assignment)
- Called from (representative examples):
  - Used with PQTRACE_REGRESS_MODE in libpq-fe.h (line 464)
  - Used in libpq_pipeline test module (line 2249)

## Notes and Other Information
- Requires an active tracing session (PQtrace must be called first)
- Performs safety checks for NULL connections and inactive tracing
- Flags are stored as a bitwise integer, allowing multiple options to be combined
- PQTRACE_SUPPRESS_TIMESTAMPS is useful for cleaner output when timestamps aren't needed
- PQTRACE_REGRESS_MODE helps create consistent output for automated testing
- Part of libpq's comprehensive tracing and debugging infrastructure

## Simplified Source

```c
void PQsetTraceFlags(PGconn *conn, int flags) {
    // Validate connection parameter
    if (conn == NULL)
        return;

    // Only set flags if tracing is active
    if (conn->Pfdebug == NULL)
        return;

    // Set the trace flags
    conn->traceFlags = flags;
}
```