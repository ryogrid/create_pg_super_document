# pqPuts

## Location
[src/interfaces/libpq/fe-misc.c:152-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L152-L164)

## Overview
pqPuts writes a null-terminated string to the current message being constructed for a PostgreSQL connection.

## Definition
```c
int pqPuts(const char *s, PGconn *conn)
```

## Detailed Description
pqPuts is a utility function that writes a null-terminated string to the connection's output message buffer. It automatically includes the null terminator in the data being written by adding 1 to the string length. The function serves as a convenient wrapper around pqPutMsgBytes for writing complete strings to PostgreSQL protocol messages.

The function calculates the total length including the null terminator using strlen(s) + 1 and delegates the actual writing to pqPutMsgBytes. This ensures that the receiving end can properly parse the string data.

## Parameters / Member Variables
- `s`: The null-terminated string to write to the message buffer
- `conn`: PGconn connection object containing the output buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pqPutMsgBytes](pqPutMsgBytes.md) (for the actual byte writing)
  - strlen (for calculating string length)
- Called from (representative examples):
  - [pg_SASL_init](pg_SASL_init.md) (fe-auth.c:591)
  - [PQsendQueryInternal](../P/PQsendQueryInternal.md) (fe-exec.c:1456)
  - [PQsendPrepare](../P/PQsendPrepare.md) (fe-exec.c:1569, 1570)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md) (fe-exec.c:1784, 1785, 1808, 1809, 1875, 1881)
  - [PQsendTypedCommand](../P/PQsendTypedCommand.md) (fe-exec.c:2607)
  - [PQputCopyEnd](../P/PQputCopyEnd.md) (fe-exec.c:2768)

## Notes and Other Information
- Returns 0 on success, EOF on failure
- Automatically includes the null terminator in the written data
- Part of the libpq client library's protocol message construction utilities
- Commonly used when building PostgreSQL wire protocol messages that require string parameters
- The function handles the common case where you want to send a complete C string including its terminator