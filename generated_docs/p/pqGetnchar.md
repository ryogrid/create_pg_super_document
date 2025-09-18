# pqGetnchar

## Location
src/interfaces/libpq/fe-misc.c: 165 - 186

## Overview
pqGetnchar reads a fixed number of bytes from a PostgreSQL connection's input buffer without null termination.

## Definition
```c
int pqGetnchar(char *s, size_t len, PGconn *conn)
```

## Detailed Description
pqGetnchar is a low-level function that reads exactly the specified number of bytes from the connection's input buffer into the provided destination buffer. Unlike string-oriented functions, it does not add null termination and treats the data as raw bytes. The function checks if sufficient data is available in the input buffer before attempting the copy operation.

The function uses memcpy for efficient data transfer and advances the connection's input cursor by the number of bytes read. This maintains proper buffer state for subsequent read operations. If insufficient data is available, the function returns EOF without modifying the buffer or cursor position.

## Parameters / Member Variables
- `s`: Destination buffer to store the read bytes
- `len`: Exact number of bytes to read from the connection
- `conn`: PGconn connection object containing the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (for byte copying)
- Called from (representative examples):
  - [pg_GSS_continue](pg_GSS_continue.md) (fe-auth.c:81)
  - [pg_SSPI_continue](pg_SSPI_continue.md) (fe-auth.c:242)
  - [pg_SASL_continue](pg_SASL_continue.md) (fe-auth.c:645)
  - [pg_password_sendauth](pg_password_sendauth.md) (fe-auth.c:710)
  - [pqFunctionCall3](pqFunctionCall3.md) (fe-protocol3.c:2144)

## Notes and Other Information
- Returns 0 on success, EOF if insufficient data is available
- Does NOT null-terminate the destination buffer
- Used for reading binary data or fixed-length fields from PostgreSQL protocol messages
- Commonly used in authentication routines and protocol parsing where exact byte counts are required
- The caller is responsible for ensuring the destination buffer has sufficient space
- Advances the connection's input cursor position after successful read