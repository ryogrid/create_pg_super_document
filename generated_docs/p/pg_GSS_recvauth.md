# pg_GSS_recvauth

## Location
[src/backend/libpq/auth.c:928-1080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L928-L1080)

## Overview
pg_GSS_recvauth implements the server-side GSS-API (Generic Security Services) authentication protocol for PostgreSQL, handling the complete GSSAPI token exchange with clients.

## Definition

```c
static int
pg_GSS_recvauth(Port *port)
```
## Detailed Description
pg_GSS_recvauth is the core function for server-side GSSAPI authentication in PostgreSQL. It establishes a GSS security context by exchanging GSSAPI tokens with the client through a multi-round protocol. The function handles Kerberos keytab configuration, manages the complete token exchange loop, processes delegated credentials if configured, and performs all necessary GSSAPI security context operations. It accepts any service principal present in the server's keytab for maximum interoperability between different Kerberos implementations.

## Parameters / Member Variables
- `*port`: Connection port structure containing client connection information and GSS-specific authentication state
## Dependencies
- Functions called/Symbols referenced:
  - setenv (configures KRB5_KTNAME environment variable for keytab)
  - [pq_startmsgread](pq_startmsgread.md), pq_getbyte, pq_getmessage (protocol message handling)
  - gss_accept_sec_context (core GSSAPI context establishment)
  - gss_release_buffer, gss_release_cred, gss_delete_sec_context (GSSAPI cleanup)
  - [pg_store_delegated_credential](pg_store_delegated_credential.md) (stores delegated credentials if enabled)
  - [sendAuthRequest](../s/sendAuthRequest.md) (sends AUTH_REQ_GSS_CONT continuation requests)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting)
  - [pg_GSS_checkauth](pg_GSS_checkauth.md) (final authentication validation)
- Called from (representative examples):
  - [ClientAuthentication](../C/ClientAuthentication.md) function in auth.c:570

## Notes and Other Information
- Supports multi-round GSSAPI token exchange as required by the protocol
- Configures Kerberos keytab via KRB5_KTNAME environment variable if pg_krb_server_keyfile is set
- Accepts any service principal in keytab for interoperability
- Handles credential delegation when pg_gss_accept_delegation is enabled
- Returns STATUS_ERROR on protocol violations or GSSAPI failures
- Performs proper cleanup of GSSAPI resources (buffers, credentials, contexts)
- Delegates final authentication checks to pg_GSS_checkauth
- Expects PqMsg_GSSResponse message types from client during token exchange
- Maximum token length limited by PG_MAX_AUTH_TOKEN_LENGTH