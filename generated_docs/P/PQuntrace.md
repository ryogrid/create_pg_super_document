# PQuntrace

## Location
src/interfaces/libpq/fe-trace.c: 49 - 63

## Overview
Disables protocol tracing for a PostgreSQL connection and ensures any pending trace output is flushed to the output stream.

## Definition
```c
void PQuntrace(PGconn *conn)
```

## Detailed Description
PQuntrace disables protocol-level tracing that was previously enabled by PQtrace. The function performs cleanup by flushing any remaining trace output to ensure all trace data is written before disabling tracing. It safely handles the case where tracing is already disabled or was never enabled. The function also resets the trace flags to ensure a clean state after tracing is disabled.

## Parameters / Member Variables
- `conn`: The PostgreSQL connection handle (PGconn *) for which tracing should be disabled

## Dependencies
- Functions called/Symbols referenced:
  - fflush (standard C library function)
- Called from (representative examples):
  - [PQtrace](PQtrace.md) (line 39 in fe-trace.c)
  - Referenced in libpq-fe.h header (line 457)

## Notes and Other Information
- Performs safety check for NULL connection pointers
- Calls fflush() to ensure all trace output is written before disabling tracing
- Sets both Pfdebug to NULL and traceFlags to 0 for complete cleanup
- Safe to call multiple times or when tracing is already disabled
- Essential for proper resource cleanup when tracing is no longer needed
- Part of the libpq tracing subsystem's resource management