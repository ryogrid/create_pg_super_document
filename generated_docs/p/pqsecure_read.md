# pqsecure_read

## Location
src/interfaces/libpq/fe-secure.c: 182 - 207

## Overview
Reads data from a secure PostgreSQL connection, automatically selecting the appropriate reading method based on the connection's security configuration (SSL, GSS, or raw).

## Definition
```c
ssize_t pqsecure_read(PGconn *conn, void *ptr, size_t len)
```

## Detailed Description
This function serves as a unified interface for reading data from PostgreSQL connections regardless of the underlying security mechanism. It intelligently dispatches to the appropriate reading function based on the connection's security state:
- If SSL is in use, it calls `pgtls_read()` for SSL/TLS encrypted reading
- If GSS encryption is enabled, it calls `pg_GSS_read()` for GSSAPI encrypted reading  
- Otherwise, it falls back to `pqsecure_raw_read()` for unencrypted reading

The function is responsible for appending appropriate error messages to `conn->errorMessage` on failure, while the caller should inspect errno to determine retry behavior.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection structure (PGconn)
- `ptr`: Buffer to store the read data
- `len`: Maximum number of bytes to read

## Dependencies
- Functions called/Symbols referenced:
  - `pgtls_read` (when SSL is in use)
  - `pg_GSS_read` (when GSS encryption is enabled)
  - `pqsecure_raw_read` (for unencrypted connections)
  - `USE_SSL` (preprocessor macro)
  - `ENABLE_GSS` (preprocessor macro)
- Called from (representative examples):
  - `pqReadData` (in fe-misc.c:642, 737)
  - `pgunlock_thread` (referenced in libpq-int.h:768)

## Notes and Other Information
- Returns the number of bytes read on success, or -1 on error
- The function abstracts away the complexity of different security protocols from the caller
- Error handling responsibility is split: this function handles error message formatting, while the caller handles errno-based retry logic
- The precedence order is SSL → GSS → raw, ensuring encrypted connections take priority