# be_gssapi_get_enc

## Location
[src/backend/libpq/be-secure-gssapi.c:753-765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-gssapi.c#L753-L765)

## Overview
Returns whether GSSAPI encryption is enabled and actively being used on the given connection.

## Definition

```c
bool
be_gssapi_get_enc(Port *port)
```
## Detailed Description
The  function is a simple query function that checks whether GSSAPI encryption is currently active for a specific connection. It performs null safety checks on the port and its GSSAPI state structure before returning the encryption status.

This function is used by other parts of PostgreSQL to determine whether communication on this connection is encrypted via GSSAPI. This is distinct from GSSAPI authentication - a connection can have GSSAPI authentication without encryption, or potentially encryption without having used GSSAPI for the initial authentication (though this is uncommon).

The encryption flag is typically set to true after  successfully completes the GSSAPI handshake and sets up the encryption context.

## Parameters / Member Variables
- `*port`: Pointer to Port structure containing connection state information
## Dependencies
- Functions called/Symbols referenced:
  - None (simple accessor function)
- Struct members accessed:
  - : GSSAPI state structure
  - : Boolean flag indicating if GSSAPI encryption is active
- Called from:
  - : For collecting connection statistics and reporting encryption status
  - : During authentication process to verify encryption state

## Notes and Other Information
- Returns  if the port is NULL or has no GSSAPI state structure
- Returns  only if GSSAPI encryption is successfully established and active
- This is separate from authentication status - encryption and authentication are tracked independently
- Used for security auditing and monitoring to ensure connections meet encryption requirements
- The  flag is set to true at the end of successful  execution
- Essential for determining when to use / vs plain socket operations

## Simplified Source

```c
// Simplified version of be_gssapi_get_enc
bool be_gssapi_get_enc(Port *port) {
    // Step 1: Validate port and GSSAPI state exist
    if (!port || !port->gss) {
        return false;
    }

    // Step 2: Return the GSSAPI encryption status
    return port->gss->enc;
}
```

Key simplifications made:
- Added step-by-step comments explaining the validation and access logic
- Maintained the essential null safety checks for port and GSSAPI structure
- Preserved the straightforward encryption status query functionality
- Kept the simple boolean return pattern consistent with other GSSAPI getter functions