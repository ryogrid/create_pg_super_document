# pg_SSPI_recvauth

## Location
[src/backend/libpq/auth.c:1206-1492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L1206-L1492)

## Overview
The main server-side function that handles SSPI (Security Support Provider Interface) authentication for PostgreSQL client connections on Windows platforms.

## Definition
```c
static int pg_SSPI_recvauth(Port *port)
```

## Detailed Description
pg_SSPI_recvauth implements the complete server-side SSPI authentication handshake for PostgreSQL. The function performs a multi-step process:

1. **Credentials Acquisition**: Acquires server credentials using the "negotiate" security package, which allows the server to authenticate incoming client requests.

2. **Token Exchange Loop**: Engages in a potentially multi-round message exchange with the client, where each message contains SSPI security tokens. The exchange continues until authentication is complete or fails.

3. **Security Context Management**: Manages SSPI security contexts throughout the authentication process, including proper cleanup on both success and failure paths.

4. **User Identity Extraction**: Upon successful authentication, extracts the authenticated user's identity from the security token, including both the account name and domain information.

5. **Identity Format Conversion**: Converts the Windows identity into either SAM-compatible format (DOMAIN\username) or Kerberos principal format (username@DOMAIN) based on configuration.

6. **Authorization Checking**: Performs final authorization checks including domain validation and user mapping according to the HBA (Host-Based Authentication) configuration.

The function handles various error conditions gracefully, ensuring proper cleanup of allocated resources and security contexts.

## Parameters / Member Variables
- `*port`: Pointer to the Port structure containing connection information, HBA configuration, and client details
## Dependencies
- Functions called/Symbols referenced:
  - AcquireCredentialsHandle (Windows SSPI API)
  - AcceptSecurityContext (Windows SSPI API)
  - QuerySecurityContextToken (Windows SSPI API)
  - GetTokenInformation (Windows API)
  - LookupAccountSid (Windows API)
  - [pg_SSPI_error](pg_SSPI_error.md)
  - [pg_SSPI_make_upn](pg_SSPI_make_upn.md)
  - [pq_startmsgread](pq_startmsgread.md)
  - [pq_getbyte](pq_getbyte.md)
  - [pq_getmessage](pq_getmessage.md)
  - [sendAuthRequest](../s/sendAuthRequest.md)
  - [set_authn_id](../s/set_authn_id.md)
  - [check_usermap](../c/check_usermap.md)
  - malloc/free
- Called from (representative examples):
  - LDAP_OPT_DIAGNOSTIC_MESSAGE context
  - HOSTNAME_LOOKUP_DETAIL context

## Notes and Other Information
- This function is Windows-specific and only compiled on Windows builds of PostgreSQL
- Uses the "negotiate" security package, which supports both NTLM and Kerberos authentication protocols
- Implements proper resource cleanup with multiple exit paths to prevent memory leaks and handle failures gracefully
- Supports both traditional SAM-compatible identity formats and modern Kerberos principal formats
- The authentication process may involve multiple round-trips between client and server
- Token buffer size is limited by PG_MAX_AUTH_TOKEN_LENGTH for security reasons
- Performs case-insensitive domain/realm comparison for SSPI authentication
- The function returns STATUS_OK on success or STATUS_ERROR on failure
- Includes extensive debug logging at DEBUG4 level for troubleshooting authentication issues