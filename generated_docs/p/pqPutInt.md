# pqPutInt

## Location
[src/interfaces/libpq/fe-misc.c:253-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L253-L286)

## Overview
Writes a 2 or 4 byte integer to the output buffer, converting from host byte order to network byte order.

## Definition
```c
int pqPutInt(int value, size_t bytes, PGconn *conn)
```

## Detailed Description
The `pqPutInt` function is the counterpart to `pqGetInt`, used in the libpq library to write integer values to the output buffer of a PostgreSQL connection. It handles both 2-byte and 4-byte integers, automatically converting them from the local machine's byte order to network byte order (big-endian). This ensures that data sent to the PostgreSQL server is in the correct format regardless of the client machine's native byte order.

The function performs the byte order conversion using appropriate conversion functions, then delegates the actual writing to `pqPutMsgBytes`. This abstraction allows for consistent handling of buffer management and error conditions across all message writing operations.

## Parameters / Member Variables
- `value`: The integer value to write to the output buffer
- `bytes`: Number of bytes to write (must be 2 or 4)
- `conn`: PostgreSQL connection object containing the output buffer

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton16 (converts 16-bit value from host to network byte order)
  - pg_hton32 (converts 32-bit value from host to network byte order)
  - [pqPutMsgBytes](pqPutMsgBytes.md) (writes raw bytes to the output buffer)
  - [pqInternalNotice](pqInternalNotice.md) (logs internal notice messages)
- Called from (representative examples):
  - [PQsendPrepare](../P/PQsendPrepare.md) (sending prepared statement creation)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md) (sending query execution requests)
  - [pqFunctionCall3](pqFunctionCall3.md) (sending function call requests)
  - [pg_SASL_init](pg_SASL_init.md) (authentication message construction)

## Notes and Other Information
- Only supports 2-byte and 4-byte integers; other sizes will result in an error notice and EOF return
- Returns 0 on success, EOF on failure (unsupported size or buffer write failure)
- The function creates temporary variables to hold the converted values before writing
- Essential for constructing PostgreSQL protocol messages which require network byte order for all integer fields
- Part of the libpq internal API, used extensively in protocol message construction
- Complements pqGetInt for bidirectional integer handling in the PostgreSQL wire protocol