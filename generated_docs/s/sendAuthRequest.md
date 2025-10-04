# sendAuthRequest

## Location
[src/backend/libpq/auth.c:684-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L684-L713)

## Overview
Sends authentication request packets to the frontend client during the authentication process, handling message formatting and transmission according to PostgreSQL's protocol.

## Definition

```c
void
sendAuthRequest(Port *port, AuthRequest areq, const char *extradata, int extralen)
```
## Detailed Description
The  function is responsible for sending authentication-related messages from the PostgreSQL backend to the client frontend. It constructs and transmits authentication request packets using PostgreSQL's message protocol system.

The function builds messages using the StringInfoData buffer system, starting with a message type identifier (PqMsg_AuthenticationRequest), followed by the authentication request type code and any additional data required for the specific authentication method.

A key optimization is implemented where most authentication messages are immediately flushed to ensure the client receives them promptly, except for AUTH_REQ_OK and AUTH_REQ_SASL_FIN messages which are deferred until the server is ready to process queries.

## Parameters / Member Variables
- `*port`: Pointer to Port structure containing connection information
- `areq`: AuthRequest enum value specifying the type of authentication request
- `*extradata`: Optional pointer to additional data to include in the message (can be NULL)
- `extralen`: Length of extra data in bytes (0 if no extra data)
## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (check for query cancellation)
  - [pq_beginmessage](../p/pq_beginmessage.md) (start building a protocol message)
  - [pq_sendint32](../p/pq_sendint32.md) (send 32-bit integer in network byte order)
  - [pq_sendbytes](../p/pq_sendbytes.md) (send raw bytes)
  - [pq_endmessage](../p/pq_endmessage.md) (finalize message construction)
  - pq_flush (immediately send buffered data to client)
- Constants used:
  - PqMsg_AuthenticationRequest (message type identifier)
  - AUTH_REQ_OK (successful authentication)
  - AUTH_REQ_SASL_FIN (SASL authentication completion)
- Called from (representative examples):
  - [CheckSASLAuth](../C/CheckSASLAuth.md) (SASL/SCRAM authentication)
  - [CheckPasswordAuth](../C/CheckPasswordAuth.md) (password authentication)
  - [CheckMD5Auth](../C/CheckMD5Auth.md) (MD5 password authentication)
  - [pg_GSS_recvauth](../p/pg_GSS_recvauth.md) (GSS/Kerberos authentication)
  - [pg_SSPI_recvauth](../p/pg_SSPI_recvauth.md) (SSPI authentication)
  - [CheckBSDAuth](../C/CheckBSDAuth.md) (BSD authentication)
  - [CheckLDAPAuth](../C/CheckLDAPAuth.md) (LDAP authentication)
  - [CheckRADIUSAuth](../C/CheckRADIUSAuth.md) (RADIUS authentication)
  - [ClientAuthentication](../C/ClientAuthentication.md) (main authentication dispatcher)

## Notes and Other Information
- The function handles both simple authentication requests (no extra data) and complex ones requiring additional parameters
- Message flushing is optimized: most messages are sent immediately, but OK and SASL_FIN messages are deferred
- Uses PostgreSQL's standard message protocol with proper byte ordering for network transmission
- Includes interrupt checking to handle query cancellation during authentication
- The extradata parameter allows for method-specific authentication data (salts, nonces, certificates, etc.)
- Part of PostgreSQL's broader authentication infrastructure that supports multiple authentication methods

## Simplified Source

```c
void
sendAuthRequest(Port *port, AuthRequest areq, const char *extradata, int extralen)
{
    StringInfoData buf;

    CHECK_FOR_INTERRUPTS();

    // Build authentication request message
    pq_beginmessage(&buf, PqMsg_AuthenticationRequest);
    pq_sendint32(&buf, (int32) areq);

    // Include any additional authentication data
    if (extralen > 0)
        pq_sendbytes(&buf, extradata, extralen);

    pq_endmessage(&buf);

    // Flush immediately for most requests, except OK and SASL_FIN
    // which are deferred until ready for queries
    if (areq != AUTH_REQ_OK && areq != AUTH_REQ_SASL_FIN)
        pq_flush();

    CHECK_FOR_INTERRUPTS();
}
```