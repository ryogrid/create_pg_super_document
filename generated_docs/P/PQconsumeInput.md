# PQconsumeInput

## Location
[src/interfaces/libpq/fe-exec.c:1984-2019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1984-L2019)

## Overview
PQconsumeInput is a public libpq function that consumes any available input data from the backend server without blocking, making it available for subsequent parsing and processing.

## Definition


## Detailed Description
PQconsumeInput is a fundamental libpq function used to read available data from the PostgreSQL server connection without blocking. It performs essential input/output management by first flushing any pending output for non-blocking connections, then reading any available input data from the network socket.

The function is designed to be called whenever the application wants to handle available network data, typically in response to select() or poll() indicating that the socket is ready for reading. It does not parse or process the data it reads - that parsing happens later when functions like PQgetResult() are called.

For non-blocking connections, the function first ensures that any buffered output is sent to avoid deadlock situations where the server might be waiting for a command that's still in the client's send buffer. After handling output, it reads whatever input data is available without blocking.

The function is essential for proper operation of asynchronous and non-blocking connection modes, and is commonly used in event loops and when polling for query completion.

## Parameters / Member Variables
- : The PostgreSQL connection handle

## Dependencies
- Functions called/Symbols referenced:
  - pqIsnonblocking
  - [pqFlush](../p/pqFlush.md)
  - [pqReadData](../p/pqReadData.md)
- Called from (representative examples):
  - [libpqrcv_PQgetResult](../l/libpqrcv_PQgetResult.md) (in libpqwalreceiver)
  - [libpqrcv_receive](../l/libpqrcv_receive.md) (in libpqwalreceiver)
  - [StreamLogicalLog](../S/StreamLogicalLog.md) (in pg_recvlogical)
  - [CopyStreamReceive](../C/CopyStreamReceive.md) (in receivelog)
  - [advanceConnectionState](../a/advanceConnectionState.md) (in pgbench)
  - [PrintNotifications](PrintNotifications.md) (in psql)
  - [wait_on_slots](../w/wait_on_slots.md) (in parallel_slot)
  - [try_complete_step](../t/try_complete_step.md) (in isolationtester)
  - Various test functions in libpq_pipeline tests

## Notes and Other Information
- Returns 1 for success, 0 for some kind of trouble/error
- Does not block waiting for input - only reads what's immediately available
- For non-blocking connections, flushes the send queue first to prevent deadlocks
- Data parsing is deferred until later - this function only handles I/O
- Essential for proper asynchronous operation and event-driven programming
- Commonly called in response to socket readiness events from select()/poll()
- Used extensively throughout PostgreSQL tools and test suites for non-blocking I/O
- The function handles connection validation (returns 0 if conn is NULL)
- Critical for maintaining responsiveness in applications that need to handle multiple connections or avoid blocking