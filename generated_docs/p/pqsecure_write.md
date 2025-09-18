# pqsecure_write

## Location
src/interfaces/libpq/fe-secure.c: 282 - 330

## Overview
Writes data to a secure PostgreSQL connection, automatically selecting the appropriate writing method based on the connection's security configuration (SSL, GSS, or raw).

## Definition
```c
ssize_t pqsecure_write(PGconn *conn, const void *ptr, size_t len)
```

## Detailed Description
This function serves as a unified interface for writing data to PostgreSQL connections regardless of the underlying security mechanism. It intelligently dispatches to the appropriate writing function based on the connection's security state:
- If SSL is in use, it calls `pgtls_write()` for SSL/TLS encrypted writing
- If GSS encryption is enabled, it calls `pg_GSS_write()` for GSSAPI encrypted writing
- Otherwise, it falls back to `pqsecure_raw_write()` for unencrypted writing

The function implements a sophisticated error handling strategy:
- Socket-level hard failures are masked and stored in `conn->write_failed` and `conn->write_err_msg` to postpone reporting until server error messages can be checked
- SSL/GSS management errors are reported immediately via negative return values and `conn->errorMessage`
- This allows the connection layer to prioritize server-provided error messages when available

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection structure (PGconn)
- `ptr`: Pointer to the data to be written (const to indicate it won't be modified)
- `len`: Number of bytes to write

## Dependencies
- Functions called/Symbols referenced:
  - [pgtls_write](pgtls_write.md) (when SSL is in use)
  - [pg_GSS_write](pg_GSS_write.md) (when GSS encryption is enabled)
  - [pqsecure_raw_write](pqsecure_raw_write.md) (for unencrypted connections)
  - `USE_SSL` (preprocessor macro)
  - `ENABLE_GSS` (preprocessor macro)
- Called from (representative examples):
  - [pqSendSome](pqSendSome.md) (in fe-misc.c:855, 863)
  - `pgunlock_thread` (referenced in libpq-int.h:769)

## Notes and Other Information
- Returns the number of bytes written on success, or -1 on error
- May return fewer bytes than requested without indicating an error
- The function abstracts away the complexity of different security protocols from the caller
- Error handling strategy differs from read operations: write errors may be deferred to allow server error messages to take precedence
- The precedence order is SSL → GSS → raw, ensuring encrypted connections take priority
- Callers should inspect errno on failure but only for retry logic, as error messages are handled internally