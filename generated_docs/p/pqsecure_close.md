# pqsecure_close

## Location
src/interfaces/libpq/fe-secure.c: 167 - 181

## Overview
Closes the secure session for a PostgreSQL connection, specifically handling SSL/TLS cleanup when SSL support is enabled.

## Definition
```c
void pqsecure_close(PGconn *conn)
```

## Detailed Description
This function handles the cleanup of secure connections in libpq. It is a wrapper function that conditionally calls the SSL-specific cleanup function `pgtls_close()` when SSL support is compiled in (when `USE_SSL` is defined). The function ensures proper closure of secure communication channels before the connection is fully terminated.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection structure (PGconn) for which the secure session should be closed

## Dependencies
- Functions called/Symbols referenced:
  - `[pgtls_close](pgtls_close.md)` (when USE_SSL is defined)
  - `USE_SSL` (preprocessor macro)
- Called from (representative examples):
  - `[pqDropConnection](pqDropConnection.md)` (in fe-connect.c:474)
  - `pgunlock_thread` (referenced in libpq-int.h:767)

## Notes and Other Information
- This function is conditionally compiled and only performs actual cleanup when SSL support is enabled
- It serves as an abstraction layer over the SSL-specific cleanup functionality
- The function is called as part of the connection teardown process in libpq
- When SSL is not compiled in, this function effectively becomes a no-op