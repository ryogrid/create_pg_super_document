# pqPutnchar

## Location
[src/interfaces/libpq/fe-misc.c:202-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L202-L215)

## Overview
pqPutnchar writes a fixed number of bytes from a buffer to the current PostgreSQL protocol message without null termination.

## Definition
```c
int pqPutnchar(const char *s, size_t len, PGconn *conn)
```

## Detailed Description
pqPutnchar is a low-level function that writes exactly the specified number of bytes from the source buffer to the connection's output message buffer. Unlike pqPuts which automatically includes null termination, this function treats the data as raw bytes and writes only the specified length. It serves as a wrapper around pqPutMsgBytes for fixed-length binary data or when precise byte control is needed.

The function is commonly used when constructing PostgreSQL wire protocol messages that contain binary data, fixed-length fields, or when the exact byte count matters (such as when null terminators should not be included). It delegates the actual writing to pqPutMsgBytes while providing a convenient interface for length-specified writes.

## Parameters / Member Variables
- `s`: Source buffer containing the bytes to write
- `len`: Exact number of bytes to write from the source buffer
- `conn`: PGconn connection object containing the output message buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pqPutMsgBytes](pqPutMsgBytes.md) (for the actual byte writing)
- Called from (representative examples):
  - [pg_SASL_init](pg_SASL_init.md) (fe-auth.c:597)
  - [pqPacketSend](pqPacketSend.md) (fe-connect.c:4994)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md) (fe-exec.c:1856)
  - [PQputCopyData](../P/PQputCopyData.md) (fe-exec.c:2734)
  - [pqFunctionCall3](pqFunctionCall3.md) (fe-protocol3.c:2050)

## Notes and Other Information
- Returns 0 on success, EOF on failure
- Does NOT add null termination to the written data
- Used for writing binary data or fixed-length fields in PostgreSQL protocol messages
- Provides precise control over the number of bytes written
- Commonly used in authentication routines and protocol message construction
- The caller is responsible for ensuring the source buffer contains at least 'len' bytes
- Complements pqGetnchar for binary data handling in the PostgreSQL client library