# sendAuthRequest

## Location
src/backend/libpq/auth.c: 684 - 713

## Overview
Sends authentication request packets to the frontend client during the authentication process, handling message formatting and transmission according to PostgreSQL's protocol.

## Definition


## Detailed Description
The  function is responsible for sending authentication-related messages from the PostgreSQL backend to the client frontend. It constructs and transmits authentication request packets using PostgreSQL's message protocol system.

The function builds messages using the StringInfoData buffer system, starting with a message type identifier (PqMsg_AuthenticationRequest), followed by the authentication request type code and any additional data required for the specific authentication method.

A key optimization is implemented where most authentication messages are immediately flushed to ensure the client receives them promptly, except for AUTH_REQ_OK and AUTH_REQ_SASL_FIN messages which are deferred until the server is ready to process queries.

## Parameters / Member Variables
- : Pointer to Port structure containing connection information
- : AuthRequest enum value specifying the type of authentication request
- : Optional pointer to additional data to include in the message (can be NULL)
- : Length of extra data in bytes (0 if no extra data)

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (check for query cancellation)
  - pq_beginmessage (start building a protocol message)
  - pq_sendint32 (send 32-bit integer in network byte order)
  - pq_sendbytes (send raw bytes)
  - pq_endmessage (finalize message construction)
  - pq_flush (immediately send buffered data to client)
- Constants used:
  - PqMsg_AuthenticationRequest (message type identifier)
  - AUTH_REQ_OK (successful authentication)
  - AUTH_REQ_SASL_FIN (SASL authentication completion)
- Called from (representative examples):
  - CheckSASLAuth (SASL/SCRAM authentication)
  - CheckPasswordAuth (password authentication)
  - CheckMD5Auth (MD5 password authentication)
  - pg_GSS_recvauth (GSS/Kerberos authentication)
  - pg_SSPI_recvauth (SSPI authentication)
  - CheckBSDAuth (BSD authentication)
  - CheckLDAPAuth (LDAP authentication)
  - CheckRADIUSAuth (RADIUS authentication)
  - ClientAuthentication (main authentication dispatcher)

## Notes and Other Information
- The function handles both simple authentication requests (no extra data) and complex ones requiring additional parameters
- Message flushing is optimized: most messages are sent immediately, but OK and SASL_FIN messages are deferred
- Uses PostgreSQL's standard message protocol with proper byte ordering for network transmission
- Includes interrupt checking to handle query cancellation during authentication
- The extradata parameter allows for method-specific authentication data (salts, nonces, certificates, etc.)
- Part of PostgreSQL's broader authentication infrastructure that supports multiple authentication methods