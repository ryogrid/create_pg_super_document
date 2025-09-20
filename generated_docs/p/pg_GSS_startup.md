# pg_GSS_startup

## Location
[src/interfaces/libpq/fe-auth.c:161-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L161-L198)

## Overview
Initiates GSS authentication by setting up the initial context and delegating to the continuation function for the first authentication exchange.

## Definition

```c
static int
pg_GSS_startup(PGconn *conn, int payloadlen)
```
## Detailed Description
This function handles the initialization phase of GSSAPI authentication for PostgreSQL client connections. It performs essential pre-authentication setup and validation before starting the actual GSS authentication handshake. The function:

1. **Host Validation**: Ensures a valid hostname is specified, which is required for GSS service name construction
2. **Duplicate Check**: Prevents multiple GSS authentication attempts on the same connection
3. **Service Name Loading**: Calls  to construct the GSS service principal name
4. **Context Initialization**: Sets up an empty GSS security context for the first authentication round
5. **Delegation**: Delegates to  to handle the actual token exchange

This separation allows the continuation function to handle both initial and subsequent authentication rounds uniformly.

## Parameters / Member Variables
- : PostgreSQL connection structure containing host information and GSS state
- : Length of any incoming authentication data (typically 0 for initial startup)

## Dependencies
- Functions called/Symbols referenced:
  -  - Error reporting for connection issues
  -  - Constructs GSS service principal name
  -  - Handles the actual authentication token exchange
- Called from (representative examples):
  -  - Main authentication dispatcher when GSS method is selected

## Notes and Other Information
- This is a static function internal to the libpq authentication module
- Requires a valid hostname to construct the GSS service principal (usually 'postgres/hostname@REALM')
- Prevents duplicate authentication attempts by checking for existing GSS context
- Sets  to indicate this is the initial authentication round
- Returns STATUS_OK on successful setup, STATUS_ERROR on validation failures
- The actual GSS token exchange is handled by delegating to 