# CheckBSDAuth

## Location
[src/backend/libpq/auth.c:2176-2218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2176-L2218)

## Overview
The CheckBSDAuth function implements BSD Authentication system integration for PostgreSQL, providing password-based authentication through the BSD auth subsystem on supported platforms.

## Definition
```c
static int CheckBSDAuth(Port *port, char *user)
```

## Detailed Description
This function implements authentication using the BSD Authentication system, which is available on BSD-derived operating systems like OpenBSD and FreeBSD. The function follows a simple authentication flow: it requests a password from the client, receives the password packet, and then uses the BSD auth_userokay() system call to validate the credentials. The BSD auth system provides a unified interface for various authentication mechanisms and can be configured through the system's authentication policies.

The function is significantly simpler than PAM authentication because the BSD auth system handles most of the complexity internally. The auth_userokay() call performs the complete authentication check and automatically zeroes out the password string for security.

## Parameters / Member Variables
- `port`: Pointer to Port structure containing connection information including socket and HBA configuration details
- `user`: Username string to authenticate against the BSD authentication system

## Dependencies
- Functions called/Symbols referenced:
  - [sendAuthRequest](../s/sendAuthRequest.md) (sends password request to client)
  - [recv_password_packet](../r/recv_password_packet.md) (receives password from client)
  - auth_userokay (BSD auth system call to verify user credentials)
  - [pfree](../p/pfree.md) (PostgreSQL memory management function to free password string)
  - [set_authn_id](../s/set_authn_id.md) (sets authenticated identity for the connection)
- Called from (representative examples):
  - HOSTNAME_LOOKUP_DETAIL (referenced in auth.c:617)

## Notes and Other Information
- Only available on platforms that support BSD Authentication (primarily BSD-derived systems)
- Uses "auth-postgresql" as the authentication style/class when calling auth_userokay
- The auth_userokay() function automatically overwrites the password string with zeroes for security
- Much simpler implementation compared to PAM authentication due to BSD auth system's unified interface
- Returns STATUS_OK on successful authentication, STATUS_ERROR on authentication failure, and STATUS_EOF if client doesn't provide password
- Sets authenticated identity using set_authn_id only after successful authentication
- Properly manages memory by freeing the password string after use
- Relies on system administrator configuration of BSD authentication policies for actual authentication mechanisms
- The function doesn't require complex conversation handling like PAM since BSD auth handles prompting internally

## Simplified Source

```c
// Simplified version of CheckBSDAuth
static int CheckBSDAuth(Port *port, char *user) {
    char *passwd;
    int retval;

    // Step 1: Request password from client
    sendAuthRequest(port, AUTH_REQ_PASSWORD, NULL, 0);

    // Step 2: Receive password packet
    passwd = recv_password_packet(port);
    if (passwd == NULL) {
        return STATUS_EOF;  // Client didn't provide password
    }

    // Step 3: Authenticate using BSD auth system
    // Note: auth_userokay overwrites password with zeroes for security
    retval = auth_userokay(user, NULL, "auth-postgresql", passwd);

    // Step 4: Clean up password memory
    pfree(passwd);

    // Step 5: Check authentication result
    if (!retval) {
        return STATUS_ERROR;  // Authentication failed
    }

    // Step 6: Set authenticated identity on success
    set_authn_id(port, user);
    return STATUS_OK;
}
```