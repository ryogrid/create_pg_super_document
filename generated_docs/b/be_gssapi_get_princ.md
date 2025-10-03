# be_gssapi_get_princ

## Location
[src/backend/libpq/be-secure-gssapi.c:766-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-gssapi.c#L766-L778)

## Overview
Returns the GSSAPI principal used for authentication on the given connection, or NULL if GSSAPI authentication was not performed.

## Definition

```c
const char *
be_gssapi_get_princ(Port *port)
```
## Detailed Description
This function retrieves the GSSAPI principal name that was used during the authentication process for a specific PostgreSQL backend connection. The function performs safety checks to ensure the port and its GSSAPI structure are valid before accessing the principal information. If GSSAPI authentication was not performed or if the connection is invalid, the function returns NULL.

The principal name is stored in the port's GSSAPI structure (port->gss->princ) and represents the authenticated identity from the GSSAPI authentication exchange.

## Parameters / Member Variables
- `*port`: A pointer to the Port structure representing the client connection. Contains all connection state information including GSSAPI authentication details.
## Dependencies
- Functions called/Symbols referenced:
  - [Port](../P/Port.md) (structure access)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (backend statistics initialization)
  - [PerformAuthentication](../P/PerformAuthentication.md) (authentication process)
  - Referenced in libpq-be.h (header declaration)

## Notes and Other Information
- Returns a const char pointer to prevent modification of the principal string
- Performs null pointer checks for both port and port->gss to ensure safe access
- The returned string is managed by the GSSAPI structure and should not be freed by the caller
- This function is part of PostgreSQL's GSSAPI authentication infrastructure
- Used primarily for logging, monitoring, and authentication verification purposes

## Simplified Source

```c
// Simplified version of be_gssapi_get_princ
const char *be_gssapi_get_princ(Port *port) {
    // Step 1: Validate port and GSSAPI state exist
    if (!port || !port->gss) {
        return NULL;
    }

    // Step 2: Return the GSSAPI principal name
    return port->gss->princ;
}
```

Key simplifications made:
- Added step-by-step comments explaining the validation and access logic
- Maintained the essential null safety checks for port and GSSAPI structure
- Preserved the straightforward principal name query functionality
- Kept the const char* return type for safe access to the principal string
- Maintained consistency with other GSSAPI getter functions