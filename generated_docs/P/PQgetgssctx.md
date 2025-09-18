# PQgetgssctx

## Location
src/interfaces/libpq/fe-secure.c: 498 - 503

## Overview
Returns the GSSAPI security context associated with a PostgreSQL client connection when GSSAPI authentication or encryption is in use.

## Definition
```c
void *PQgetgssctx(PGconn *conn)
```

## Detailed Description
This function provides access to the underlying GSSAPI security context (gss_ctx_id_t) established during GSSAPI authentication or when using GSSAPI encryption with a PostgreSQL server. The GSSAPI context contains security credentials, encryption keys, and other security state information needed for authenticated and encrypted communication.

The returned context can be used by applications that need to perform additional GSSAPI operations or integrate with other GSSAPI-aware libraries. The context is managed internally by libpq and should not be modified or freed by the application.

The function returns the context as a void pointer to maintain API compatibility and avoid exposing GSSAPI types in the public libpq interface for builds that don't have GSSAPI support.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle. Must be a valid connection object that potentially has GSSAPI authentication/encryption enabled.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple field access)
- Called from (representative examples):
  - Applications needing access to GSSAPI security context for advanced operations
- Connection fields accessed:
  - conn->gctx (GSSAPI security context field)

## Notes and Other Information
- Returns NULL if the connection parameter is NULL
- Returns the value of conn->gctx, which is of type gss_ctx_id_t internally
- The returned context is only valid when GSSAPI authentication or encryption is active
- If GSSAPI is not in use, the returned context may be GSS_C_NO_CONTEXT
- Applications should verify GSSAPI is in use with PQgssEncInUse() before using the context
- The context remains valid for the lifetime of the connection
- Only available when PostgreSQL is compiled with GSSAPI support (ENABLE_GSS)
- The underlying gss_ctx_id_t type is defined by the system's GSSAPI implementation (typically MIT Kerberos or Heimdal)
- Applications using this function should include appropriate GSSAPI headers (gssapi.h) and link against GSSAPI libraries
- The security context contains sensitive security information and should be handled appropriately